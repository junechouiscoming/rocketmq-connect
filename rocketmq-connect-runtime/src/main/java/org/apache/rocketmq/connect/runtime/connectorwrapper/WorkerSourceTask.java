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
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.api.source.SourceTaskContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.ConnectController;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper of {@link SourceTask} for runtime.
 */
public class WorkerSourceTask implements WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

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
            state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
            log.info(String.format("Source task is stopping, config:%s",this));
        } catch (Throwable e) {
            exception = e;
            state.compareAndSet(WorkerTaskState.RUNNING,WorkerTaskState.ERROR);
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

        for (SourceDataEntry sourceDataEntry : sourceDataEntries) {
            //这个partition在kafka connect中 = record.topic() + "-" + record.partition()
            ByteBuffer partition = sourceDataEntry.getSourcePartition();
            //position在kafka connect中 = record.offset()
            ByteBuffer position = sourceDataEntry.getSourcePosition();
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
            sourceMessage.setBody(value);
            //header
            if (header!=null) {
                List<String> hLst = new ArrayList<>();
                for (Map.Entry<String, byte[]> entry : header.entrySet()) {
                    hLst.add(entry.getKey());
                    sourceMessage.putUserProperty(entry.getKey(),new String(entry.getValue()));
                }
            }

            //TODO 如果第一次这里消息send失败了，那么第二条消息拉回来在这里send，会不会覆盖掉。导致漏消息。
            //理论上拉取消息时候应该指定位移把

            sendCallback = new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    log.info("Successful send message to RocketMQ:{}", result.getMsgId());
                    try {
                        if (null != partition && null != position) {
                            //对于kafka-connect这里partition就是topic+分区号,positio就是record的offset
                            //如果这里发送成功，但是callback因为宕机等没能执行，那么这条消息的位移无法提交，也就是positionManagementService无法提交，
                            // 那么下次拉取消息已更改还是会从上个位置拉取poll，那么就会造成消息再次投递到rocketMQ导致重复了
                            positionManagementService.putPosition(partition, position);
                        }
                    } catch (Exception e) {
                        log.error("Source task save position info failed.", e);
                    }
                }

                @Override
                public void onException(Throwable throwable) {
                    if (null != throwable) {
                        log.error("Source task send record failed {}.", throwable);
                    }
                }
            };

            try {
                //需要设置messageQueueSelector 如果kafka的key不null的话,这样从kafka拉下来就都能send到同一个rocketMQ的queue里了
                if (sourceMessage.getKeys()!=null && sourceMessage.getKeys().length()>0) {
                    producerToRocketMQ.send(sourceMessage, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            int i = toPositive(murmur2(sourceMessage.getKeys().getBytes(StandardCharsets.UTF_8))) % mqs.size();
                            return mqs.get(i);
                        }
                    },sendCallback);
                }else{
                    producerToRocketMQ.send(sourceMessage,sendCallback);
                }
            } catch (Exception e) {
                log.error("Send message error. message: {}, error info: {}.", sourceMessage, e);
                throw new RuntimeException(e);
            }
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
