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
import com.google.common.primitives.Longs;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.api.source.SourceTaskContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.queue.ConcurrentTreeMap;
import org.apache.rocketmq.connect.runtime.ConnectController;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper of {@link SourceTask} for runtime.
 */
public class WorkerSourceTask implements WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private static Logger logger4SourceMsg = LoggerFactory.getLogger("logger4SourceMsg");

    /**
     * Connector name of current task.
     */
    private String connectorName;

    /**
     * The implements of the source task.
     */
    private SourceTask sourceTask;

    /**
     * The configs of current source task.
     */
    private ConnectKeyValue taskConfig;

    /**
     * Atomic state variable
     */
    private AtomicReference<WorkerTaskState> state;

    private final PositionManagementService positionManagementService;

    /**
     * Used to read the position of source data source.
     */
    private PositionStorageReader positionStorageReader;

    /**
     * A RocketMQ producer to send message to dest MQ.
     */
    private DefaultMQProducer producerToRocketMQ;

    /**
     * A converter to parse source data entry to byte[].
     */
    private Converter recordConverter;

    /**
     * 这个是统一的一个开关把，不是这个task自己的state
     */
    private final AtomicReference<WorkerState> workerState;

    private final ClassLoader classLoader;

    /**
     * @param classLoader pluginClassLoader或者appClassLoader
     */
    public WorkerSourceTask(String connectorName,
        SourceTask sourceTask,
        ConnectKeyValue taskConfig,
        PositionManagementService positionManagementService,
        Converter recordConverter,
        DefaultMQProducer producerToRocketMQ,
        AtomicReference<WorkerState> workerState,
        ClassLoader classLoader) {
        this.connectorName = connectorName;
        this.sourceTask = sourceTask;
        this.taskConfig = taskConfig;
        this.positionManagementService = positionManagementService;
        this.positionStorageReader = new PositionStorageReaderImpl(positionManagementService);
        this.producerToRocketMQ = producerToRocketMQ;
        this.recordConverter = recordConverter;
        this.state = new AtomicReference<>(WorkerTaskState.NEW);
        this.workerState = workerState;
        this.classLoader = classLoader;
    }
    public WorkerSourceTask(String connectorName,
                            SourceTask sourceTask,
                            ConnectKeyValue taskConfig,
                            PositionManagementService positionManagementService,
                            Converter recordConverter,
                            DefaultMQProducer producerToRocketMQ,
                            AtomicReference<WorkerState> workerState){
        this(connectorName,sourceTask,taskConfig,positionManagementService,recordConverter, producerToRocketMQ,workerState,Thread.currentThread().getContextClassLoader());
    }
    /**
     * Start a source task, and send data entry to MQ cyclically.
     */
    @Override
    public void run() {
        ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
        state.compareAndSet(WorkerTaskState.NEW, WorkerTaskState.PENDING);
        Throwable exception = null;
        try {
            log.info(String.format("Source task is pending, config:%s",this));

            //线程运行时候,类加载器必须得设置为pluginClassLoader,否则下面的sourceTask跑不起来
            //对于其他代码应该是无影响的，因为pluginClassLoader的父类加载器是appClassLoader
            Plugin.compareAndSwapLoaders(classLoader);
            producerToRocketMQ.start();

            sourceTask.initialize(new SourceTaskContext() {
                @Override
                public PositionStorageReader positionStorageReader() {
                    return positionStorageReader;
                }
                @Override
                public KeyValue configs() {
                    return taskConfig;
                }
            });
            sourceTask.start(taskConfig);

            state.compareAndSet(WorkerTaskState.PENDING, WorkerTaskState.RUNNING);
            log.info(String.format("Source task is running, config:%s",this));

            //running area
            while (WorkerState.STARTED == workerState.get() && WorkerTaskState.RUNNING == state.get()) {
                try{
                    Collection<SourceDataEntry> toSendEntries = sourceTask.poll();
                    if (null != toSendEntries && toSendEntries.size() > 0) {
                        sendRecord(toSendEntries);
                    }
                }catch (Exception ex){
                    log.info("",ex);
                }
            }
            //normally stop area
            log.info(String.format("Source task is stopping, config:%s",this));
            state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
        } catch (Throwable e) {
            exception = e;
            state.set(WorkerTaskState.ERROR);
        } finally {
            //release resource area
            try{
                producerToRocketMQ.shutdown();
            }catch (Exception ex){
                log.error("",ex);
            }

            try{
                sourceTask.stop();
            }catch (Exception ex){
                log.error("",ex);
            }

            state.compareAndSet(WorkerTaskState.STOPPING, WorkerTaskState.STOPPED);

            if (exception==null) {
                log.info(String.format("Source task is stopped, config:%s",this));
            }else{
                log.error(String.format("Source task is stopped cuz error, config:%s",this),exception);
            }
            Plugin.compareAndSwapLoaders(currentLoader);
        }
    }

    @Override
    public void stop() {
        log.info(String.format("task with config:{%s} will stop",this));
        state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
    }


    /**
     * Send list of sourceDataEntries to MQ.
     *
     * @param sourceDataEntries
     */
    private void sendRecord(Collection<SourceDataEntry> sourceDataEntries) {
        SendCallback sendCallback;

        final Map<String/*topic*/, ConcurrentSkipListMap<Long/*position*/,String/*msgId*/>> callBackOffsetTreeMap = new ConcurrentHashMap<>(10);
        Map<String/*topic*/, TreeSet<Long/*position*/>> sendOffsetTeeSet = new HashMap<>(callBackOffsetTreeMap.size());

        boolean successAll = true;
        CountDownLatch countDownLatch = new CountDownLatch(sourceDataEntries.size());

        for (SourceDataEntry sourceDataEntry : sourceDataEntries) {
            try {
                //这个partition在kafka connect中 = record.topic() + "-" + record.partition()
                final ByteBuffer partition = sourceDataEntry.getSourcePartition();
                //position在kafka connect中 = record.offset()
                final ByteBuffer position = sourceDataEntry.getSourcePosition();
                sourceDataEntry.setSourcePartition(null);
                sourceDataEntry.setSourcePosition(null);
                Message sourceMessage = new Message();
                //mz 这里的queueName其实是topic名称
                sourceMessage.setTopic(sourceDataEntry.getQueueName());

                byte[] key = (byte[])sourceDataEntry.getPayload()[0];
                byte[] value = (byte[])sourceDataEntry.getPayload()[1];
                Map<String,byte[]> header = (Map<String,byte[]>)sourceDataEntry.getPayload()[2];

                if (key!=null && key.length>0) {
                    sourceMessage.setKeys(new String(key));
                }
                sourceMessage.putUserProperty("by_connector","true");
                sourceMessage.setBody(value.length==0?"default empty body for no exception to send".getBytes(StandardCharsets.UTF_8):value);
                //header
                if (header!=null) {
                    List<String> hLst = new ArrayList<>();
                    for (Map.Entry<String, byte[]> entry : header.entrySet()) {
                        hLst.add(entry.getKey());
                        sourceMessage.putUserProperty(entry.getKey(),new String(entry.getValue()));
                    }
                }
                final String partitionStr = new String(partition.array());
                callBackOffsetTreeMap.putIfAbsent(partitionStr, new ConcurrentSkipListMap<>());

                sendOffsetTeeSet.putIfAbsent(partitionStr, new TreeSet<>());

                final String positionStr = new String(position.array());
                TreeSet<Long> treeSet = sendOffsetTeeSet.get(partitionStr);
                treeSet.add(Long.parseLong(positionStr));

                //拉取消息时候指定位移
                sendCallback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult result) {
                        try {
                            if (result.getSendStatus() != SendStatus.SEND_OK) {
                                log.warn("not store ok send message to RocketMQ: kafka offset:{},rocketMQ msg:{}", partitionStr +":"+ positionStr,sourceMessage);
                                return;
                            }

                            final ConcurrentSkipListMap<Long, String> skipListMap = callBackOffsetTreeMap.get(partitionStr);
                            if (skipListMap==null) {
                                log.warn("ignore offset cuz timeOut");
                                //如果发现它没了,则一定是countLatch超时了被清理掉了,那么不需要提交位移
                                return;
                            }
                            //比如从P1分区拉下来5条消息，其中序号5的消息最先回来，那么这里位移就会被序号1的盖掉,所以序号1没放进去之前,序号5不准放进去，利用treeMap
                            skipListMap.put(Long.parseLong(positionStr), result.getMsgId());
                        }finally {
                            countDownLatch.countDown();
                        }
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        countDownLatch.countDown();
                        if (null != throwable) {
                            log.warn("failed send message to RocketMQ: kafka offset:{},rocketMQ msg:{}", partitionStr +":"+ positionStr,sourceMessage);
                        }
                    }
                };
                try {
                    //需要设置messageQueueSelector 如果kafka的key不null的话,这样从kafka拉下来就都能send到同一个rocketMQ的queue里了
                    if (sourceMessage.getKeys()!=null && sourceMessage.getKeys().length()>0) {
                        //带key的消息一定要保证顺序性
                        final SendResult sendResult = producerToRocketMQ.send(sourceMessage, new MessageQueueSelector() {
                            @Override
                            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                                int i = toPositive(murmur2(sourceMessage.getKeys().getBytes(StandardCharsets.UTF_8))) % mqs.size();
                                return mqs.get(i);
                            }
                        }, null);
                        sendCallback.onSuccess(sendResult);
                    }else{
                        producerToRocketMQ.send(sourceMessage,sendCallback);
                    }
                } catch (Exception e) {
                    throw new MQClientException("Send message to rocketMQ error. message: {}",e);
                }
            }catch (Exception ex){
                //任何一条有失败,break掉继续重新拉,但尝试提交一次位移
                successAll = false;
                log.warn(ex.getMessage(),ex);
                break;
            }
        }

        try {
            if (successAll) {
                //全部成功才要wait一会儿
                countDownLatch.await(10 * 60 * 1000, TimeUnit.MILLISECONDS);
            }else{
                //如果没有全部成功,直接尝试提交位移然后开始下一次重新消费
            }
        } catch (InterruptedException e) {
            log.warn("",e);
        }finally {
            //尝试提交位移,能提交多少算多少
            for (Map.Entry<String, TreeSet<Long>> entry : sendOffsetTeeSet.entrySet()) {
                final String partitionStr = entry.getKey();
                final TreeSet<Long> sendOffSet = entry.getValue();
                final ConcurrentSkipListMap<Long, String> callBackSkipMap = callBackOffsetTreeMap.get(partitionStr);
                //总共发出去的消息,减去已收到的,结果就是未回来的消息,找最后一个回来的消息提交它的位移即可
                Long lastOffset = null;
                //1 2 3
                //1 2
                for (Long offset : sendOffSet) {
                    final String msgId = callBackSkipMap.get(offset);
                    if (msgId!=null) {
                        lastOffset = offset;
                        if (ConnectConfig.isLogMsgDetail()) {
                            logger4SourceMsg.info("Successful send message to RocketMQ: kafka offset:{},rocketMQ msgID:{}", partitionStr +":"+offset,msgId);
                        }
                    }else{
                        logger4SourceMsg.error(String.format("some msg call back not enter %s:%s",partitionStr,offset));
                        //没回来,直接就可以更新位移了
                        break;
                    }
                }
                if (lastOffset!=null) {
                    logger4SourceMsg.info(String.format("positionManagementService putPosition %s:%s",partitionStr,lastOffset));
                    positionManagementService.putPosition(ByteBuffer.wrap(partitionStr.getBytes(StandardCharsets.UTF_8)),ByteBuffer.wrap(String.valueOf(lastOffset).getBytes(StandardCharsets.UTF_8)));
                }
            }
            sendOffsetTeeSet = null;
            callBackOffsetTreeMap.clear();
        }
    }

    @Override
    public WorkerTaskState getState() {
        return this.state.get();
    }

    @Override
    public String getConnectorName() {
        return connectorName;
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


    /**
     * 把kafka的搬过来了
     */
    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }
}
