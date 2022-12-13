/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.common.QueueMetaData;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.connector.api.sink.SinkTask;
import io.openmessaging.connector.api.sink.SinkTaskContext;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.ConnectController;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A wrapper of {@link SinkTask} for runtime.
 * 不要重写equals方法
 */
public class WorkerSinkTask implements WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private static Logger logger4SinkMsg = LoggerFactory.getLogger("logger4SinkMsg");

    /**
     * The configuration key that provides the list of topicNames that are inputs for this SinkTask.
     */
    public static final String QUEUENAMES_CONFIG = "rocketmq.topic";

    /**
     * Connector name of current task.
     */
    private String connectorName;

    /**
     * The implements of the sink task.
     */
    private SinkTask sinkTask;

    /**
     * The configs of current sink task.
     */
    private ConnectKeyValue taskConfig;
    /**
     * Atomic state variable
     */
    private AtomicReference<WorkerTaskState> state;

    //启动定时任务提交位移 共享这个单线程
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    /**
     * A RocketMQ consumer to pull message from MQ.
     */
    private final DefaultMQPullConsumer consumerPullRocketMQ;

    private final PositionManagementService offsetManagementService;
    /**
     *
     */
    private final PositionStorageReader offsetStorageReader;

    /**
     * A converter to parse sink data entry to object.
     */
    private Converter recordConverter;

    private final ConcurrentHashMap<MessageQueue, Long/*下次要消费的位移位置*/> messageQueuesOffsetMap;
    private final ConcurrentHashMap<MessageQueue, Long/*停止1秒*/> messageQueuesSuspendWhileMap = new ConcurrentHashMap<>();

    /**
     * 是否暂停消费
     */
    private final ConcurrentHashMap<MessageQueue, QueueState> messageQueuesStateMap;


    private static final String COMMA = ",";

    private long lastCommitTime = 0;

    private final AtomicReference<WorkerState> workerState;

    private final ClassLoader classLoader;
    /**
     * 避免GC
     */
    private static final Integer MAX_MESSAGE_NUM = 64;

    public WorkerSinkTask(String connectorName,
                          SinkTask sinkTask,
                          ConnectKeyValue taskConfig,
                          PositionManagementService offsetManagementService,
                          Converter recordConverter,
                          DefaultMQPullConsumer consumerPullRocketMQ,
                          AtomicReference<WorkerState> workerState,
                          ClassLoader classLoader) {
        this.connectorName = connectorName;
        this.sinkTask = sinkTask;
        this.taskConfig = taskConfig;
        this.consumerPullRocketMQ = consumerPullRocketMQ;
        this.offsetManagementService = offsetManagementService;
        this.offsetStorageReader = new PositionStorageReaderImpl(offsetManagementService);
        this.recordConverter = recordConverter;
        this.messageQueuesOffsetMap = new ConcurrentHashMap<>(256);
        this.messageQueuesStateMap = new ConcurrentHashMap<>(256);
        this.state = new AtomicReference<>(WorkerTaskState.NEW);
        this.workerState = workerState;
        this.classLoader = classLoader;
    }

    /**
     * Start a sink task, and receive data entry from MQ cyclically.
     */
    @Override
    public void run() {
        Plugin.compareAndSwapLoaders(this.classLoader);

        state.compareAndSet(WorkerTaskState.NEW, WorkerTaskState.PENDING);
        log.info(String.format("Sink task is pending, config:%s",this));
        Throwable exception = null;
        //pending area
        ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
        try {
            sinkTask.initialize(new SinkTaskContext() {
                /**
                 * Reset the consumer offset for the given queue.
                 */
                @Override
                public void resetOffset(QueueMetaData queueMetaData, Long offset) {}
                @Override
                public void resetOffset(Map<QueueMetaData, Long> offsets) {}
                @Override
                public void pause(List<QueueMetaData> queueMetaDatas) {}
                @Override
                public void resume(List<QueueMetaData> queueMetaDatas) {}
                @Override
                public KeyValue configs() {
                    return taskConfig;
                }
            });

            String topicNamesStr = taskConfig.getString(QUEUENAMES_CONFIG);
            //自定义的sink offset
            Consumer<String> updateOffsetByStore = topic -> {

                for (Map.Entry<MessageQueue, Long> entry : messageQueuesOffsetMap.entrySet()) {
                    MessageQueue messageQueue = entry.getKey();
                    //读真正的rocketMQ上存储的自定义的sink端消费偏移量
                    if (messageQueue.getTopic().equals(topic) || topic==null) {
                        ByteBuffer byteBuffer = offsetStorageReader.getPosition(convertToByteBufferKey(messageQueue));
                        if (null != byteBuffer) {
                            //这里为什么又要把messageQueuesOffsetMap的偏移量覆盖掉呢？
                            //是因为刚刚put的offset是rocketMQ的角度看的，而这里的offset是sink端处理的消费位移，这两个未必一样，所以如果sink处理失败，还是以sink的位移为准咯
                            messageQueuesOffsetMap.put(messageQueue, convertToOffset(byteBuffer));
                        }
                    }
                }
            };
            sinkTask.start(taskConfig);

            String[] topicNames = topicNamesStr.split(COMMA);
            for (String topicName : topicNames) {
                //注册多次，但只会有最后一个listener生效，不过topic是add到set集合的
                consumerPullRocketMQ.registerMessageQueueListener(topicName, new MessageQueueListener() {
                    /**
                     * @param mqDivided 分配给自己的
                     */
                    @Override
                    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                        //负载均衡发生时先提交一次位移,但此时消息还在拉取。所以考虑加个lock锁一下,此时别的节点可能已经开始消费数据并提交位移了，但是别的节点的位移可能有点延迟，还比较早，就会发生重复
                        commitOffset();

                        //只清空当前topic的queue
                        if (messageQueuesOffsetMap.size()>0) {
                            for (Map.Entry<MessageQueue, Long> entry : messageQueuesOffsetMap.entrySet()) {
                                if (entry.getKey().getTopic().equals(topic)) {
                                    messageQueuesOffsetMap.remove(entry.getKey());
                                }
                            }
                        }

                        for (MessageQueue messageQueue : mqDivided) {
                            try {
                                final long offset = consumerPullRocketMQ.fetchConsumeOffset(messageQueue,true);
                                //因为rocketMQ是手动提交位移，且只提交messageQueuesOffsetMap的位移，也就意味着一定已经发送到kafka了
                                messageQueuesOffsetMap.put(messageQueue,offset);
                            }catch (Exception ex){
                                log.error("consumer fetchConsumeOffset failed",ex);
                            }
                            //再用自定义的sink offset覆盖一下
                            updateOffsetByStore.accept(topic);
                        }
                    }
                });
            }
            consumerPullRocketMQ.start();

            scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    commitOffset();
                }
            },3000,5000, TimeUnit.MILLISECONDS);

            state.compareAndSet(WorkerTaskState.PENDING, WorkerTaskState.RUNNING);

            log.info(String.format("Sink task is running, config:%s",this));

            //running area
            while (WorkerState.STARTED == workerState.get() && WorkerTaskState.RUNNING == state.get()) {
                // this method can block up to 3 minutes long
                if (messageQueuesOffsetMap.size()==0) {
                    //没可以拉的queue就等1秒再拉
                    Thread.sleep(1000);
                    if (messageQueuesOffsetMap.size()==0) {
                        continue;
                    }
                }
                try {
                    pullMessageFromQueues();
                }catch (Exception ex){
                    log.error("there is an error but will continue consume msg again until it success",ex);
                }
            }

            //normally stop area
            log.info(String.format("Sink task is stopping, config:%s",this));
            state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
        } catch (Exception e) {
            log.info(String.format("Sink task is error, config:%s",this),e);
            state.set(WorkerTaskState.ERROR);
            exception = e;
        } finally {
            //release resource area
            try{
                scheduledExecutorService.shutdown();
                scheduledExecutorService.awaitTermination(2 * 1000 * 60, TimeUnit.MILLISECONDS);
            }catch (Exception ex){
                log.warn("",ex);
            }

            try {
                commitOffset();
            }catch (Exception ex){
                log.error("sink task commitOffset when stop failed",ex);
            }

            try {
                consumerPullRocketMQ.shutdown();
            }catch (Exception ex){
                log.error("",ex);
            }

            try {
                //sinkTask也要关闭啊 ，这源码写的问题也太多了把。
                sinkTask.stop();
            }catch (Exception ex){
                log.error("",ex);
            }

            state.compareAndSet(WorkerTaskState.STOPPING, WorkerTaskState.STOPPED);

            if (exception==null) {
                log.info(String.format("Sink task is stopped, config:%s",this));
            }else{
                log.error(String.format("Sink task is stopped cuz error, config:%s",this),exception);
            }
            Plugin.compareAndSwapLoaders(currentLoader);
        }
    }

    private void pullMessageFromQueues() throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        long nextPullTimestamp  = 0L ;
        for (MessageQueue queue : messageQueuesOffsetMap.keySet()) {
            Long next = messageQueuesSuspendWhileMap.get(queue);
            if (next==null) {
                nextPullTimestamp = 0L;
                break;
            }else{
                nextPullTimestamp = Math.min(nextPullTimestamp,next==null?0:next);
            }
        }

        if (System.currentTimeMillis() > nextPullTimestamp) {
            //continue pull
        }else{
            //所有的queue都没消息则sleep
            final long sleep = nextPullTimestamp - System.currentTimeMillis();
            if (sleep > 1000) {
                Thread.sleep(sleep);
            }
        }

        log.debug("START pullMessageFromQueues...");

        for (Map.Entry<MessageQueue, Long> entry : messageQueuesOffsetMap.entrySet()) {
            if (messageQueuesStateMap.containsKey(entry.getKey())) {
                continue;
            }

            if (WorkerTaskState.RUNNING != state.get()) {
                break;
            }

            final Long nextPullTime = messageQueuesSuspendWhileMap.get(entry.getKey());
            if (nextPullTime!=null && nextPullTime > System.currentTimeMillis()) {
                continue;
            }
            logger4SinkMsg.info("consumerPullRocketMQ pull offset {}:{}",entry.getKey(),entry.getValue());
            final PullResult pullResult = consumerPullRocketMQ.pull(entry.getKey(), "*", entry.getValue(), MAX_MESSAGE_NUM);

            if (pullResult.getPullStatus().equals(PullStatus.FOUND)) {
                final List<MessageExt> messages = pullResult.getMsgFoundList();
                //调用sink.put()进行处理,如果这里抛出异常，那么就下面也不会走了。只要这里不抛异常,后面正常提交位移发到rocketMQ上面去
                //如果抛出异常，那么不会提交位移
                try {
                    receiveMessages(messages);
                }catch (Throwable ex){
                    //如果抛出异常,每个Queue按照之前的offset再重新消费一次 直到成功或者任务被手动终止，这里continue掉不更新位移，然后继续消费下一个messageQueue
                    //TODO 发送到告警信息里面
                    messageQueuesSuspendWhileMap.put(entry.getKey(), System.currentTimeMillis() + 1000);
                    logger4SinkMsg.error("handle receiveMessages failed batch {}",messages);
                    continue;
                }
                if (ConnectConfig.isLogMsgDetail()) {
                    for (MessageExt messageExt : messages) {
                        final String byConnector = messageExt.getUserProperty("by_connector");
                        if (!Boolean.parseBoolean(byConnector)) {
                            logger4SinkMsg.info("successful to send msg to kafka msgId:"+messageExt.getMsgId());
                        }
                    }
                }

                //更新消费位移,如果此时已经发生重平衡,原先的queue不属于自己了,那么位移还是要提交的。这里一定会造成消息重复。另外原本的rocketMQ的offset提交机制应该也会重复。
                messageQueuesOffsetMap.put(entry.getKey(), pullResult.getNextBeginOffset());
                logger4SinkMsg.info("messageQueuesOffsetMap put offset {}:{} ",entry.getKey(),pullResult.getNextBeginOffset());
                //放到这个service里面的会同步到rocketMQ上其他节点 有必要吗？大家都是同一个消费组，既然是同一个消费组那么位移本来就在broker有保存，何必同步给其他节点？
                offsetManagementService.putPosition(convertToByteBufferKey(entry.getKey()), convertToByteBufferValue(pullResult.getNextBeginOffset()));
                logger4SinkMsg.info("offsetManagementService put offset {}:{} ",entry.getKey(),pullResult.getNextBeginOffset());
            }else{
                //如果本queue没拉到消息就延迟5秒
                messageQueuesSuspendWhileMap.put(entry.getKey(), System.currentTimeMillis() + 5000);
            }
        }
    }

    /**
     * rocketMQ本身的消费位移必须持久化，因为rocketMQ的消息是会过期删除的，比如7天后再连上来，变成重头开始消费了显然不行。
     * 另外自己上线以后，会发ONLINE消息出去，然后别的节点收到消息就会推一次offset的消息过来，本节点就可以直接更新缓存了，因为本地json文件肯定是落后了一点点它是定时持久化的
     */
    private void commitOffset() {
        //提交位移,对于fileSinkTask而言，这个就是调用一个flush操作,应该只是一个钩子函数，理论上sink端的位移提交框架要自动处理的.这里注掉，完全没用
        for (Map.Entry<MessageQueue, Long/*下次要消费的位移位置*/> entry : messageQueuesOffsetMap.entrySet()) {
            try {
                if (entry.getValue()==null) {
                    continue;
                }
                consumerPullRocketMQ.updateConsumeOffset(entry.getKey(),entry.getValue());
                logger4SinkMsg.info(String.format("consumerPullRocketMQ commit offset finish %s:%s", entry.getKey(),entry.getValue()));
            } catch (MQClientException e) {
                logger4SinkMsg.error(String.format("consumerPullRocketMQ commit offset failed %s:%s", entry.getKey(),entry.getValue()),e);
            }
        }
    }
    @Override
    public void stop() {
        log.info(String.format("task with config:{%s} will stop",this));
        state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
    }

    /**
     * receive message from MQ.
     *
     * @param messages
     */
    private void receiveMessages(List<MessageExt> messages) {
        final List<SinkDataEntry> sinkDataEntries = new ArrayList<>(MAX_MESSAGE_NUM);
        for (MessageExt message : messages) {
            SinkDataEntry sinkDataEntry = convertToSinkDataEntry(message);
            sinkDataEntries.add(sinkDataEntry);
        }
        sinkTask.put(sinkDataEntries);
        sinkDataEntries.clear();
    }

    /**
     * 原来的是OLD结尾,这里处理一下,拉下来的消息就是普通消息，不是SourceDataEntry.
     * 如果是老的kafka sender改造为rocketMQ,那么理论上应该可以完美从rocketMQ消息格式转换到kafka格式再发回去，tags理论上没用，注意kafka消息的header和key正常转换即可
     * 如果是sender直接利用了rocketMQ发送全新的消息用了新的特性例如tags，那么对方也不会去kafka消费这条消息，也没关系。所以不需要支持tags
     * @param message
     * @return
     */
    private SinkDataEntry convertToSinkDataEntry(MessageExt message) {
        String topic = message.getTopic();
        String keys = message.getKeys();
        byte[] body = message.getBody();
        Map<String, String> properties = message.getProperties();

        Schema schema = new Schema();
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(0, "key", FieldType.STRING));
        fields.add(new Field(1, "value", FieldType.BYTES));
        fields.add(new Field(2, "header", FieldType.MAP));
        schema.setName(topic);
        schema.setFields(fields);
        schema.setDataSource(topic);

        DataEntryBuilder dataEntryBuilder = new DataEntryBuilder(schema);
        dataEntryBuilder.entryType(EntryType.CREATE);
        dataEntryBuilder.queue(topic);
        dataEntryBuilder.timestamp(System.currentTimeMillis());

        dataEntryBuilder.putFiled("key",keys);
        dataEntryBuilder.putFiled("value",body);
        dataEntryBuilder.putFiled("header",properties);

        SinkDataEntry sinkDataEntry = dataEntryBuilder.buildSinkDataEntry(message.getQueueOffset());
        return sinkDataEntry;
    }

    @Override
    public String getConnectorName() {
        return connectorName;
    }

    @Override
    public WorkerTaskState getState() {
        return state.get();
    }

    @Override
    public ConnectKeyValue getTaskConfig() {
        return taskConfig;
    }

    @Override
    public String toString() {
        Map map = new LinkedHashMap();
        map.put("connectorName", connectorName);
        map.put("configs", taskConfig);
        map.put("State", state.get().toString());
        return "\n"+JSON.toJSONString(map, SerializerFeature.PrettyFormat);
    }

    @Override
    public Object getJsonObject() {
        HashMap obj = new HashMap<String, Object>();
        obj.put("connectorName", connectorName);
        obj.put("taskConfig", taskConfig);
        obj.put("state", state.get().toString());
        obj.put("workerId", ConnectController.getInstance().getConnectConfig().getWorkerId());
        return obj;
    }

    private enum QueueState {
        PAUSE
    }

    private ByteBuffer convertToByteBufferKey(MessageQueue messageQueue) {
        return ByteBuffer.wrap((messageQueue.getTopic() + COMMA + messageQueue.getBrokerName() + COMMA + messageQueue.getQueueId()).getBytes());
    }

    private MessageQueue convertToMessageQueue(ByteBuffer byteBuffer) {
        byte[] array = byteBuffer.array();
        String s = String.valueOf(array);
        String[] split = s.split(COMMA);
        return new MessageQueue(split[0], split[1], Integer.valueOf(split[2]));
    }

    private ByteBuffer convertToByteBufferValue(Long offset) {
        return ByteBuffer.wrap(String.valueOf(offset).getBytes());
    }

    private Long convertToOffset(ByteBuffer byteBuffer) {
        return Long.valueOf(new String(byteBuffer.array()));
    }
}
