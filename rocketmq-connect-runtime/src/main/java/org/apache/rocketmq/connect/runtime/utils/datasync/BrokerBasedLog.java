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

package org.apache.rocketmq.connect.runtime.utils.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.openmessaging.connector.api.data.Converter;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine.MAX_MESSAGE_SIZE;

/**
 * A Broker base data synchronizer, synchronize data between workers.
 *
 * @param <K>
 * @param <V>
 */
public class BrokerBasedLog<K, V> implements DataSynchronizer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    public static final String WORKER_ID = "workerId";
    public static final String VALUE = "value";

    /**
     * A callback to receive data from other workers.
     */
    private DataSynchronizerCallback<K, V> dataSynchronizerCallback;

    /**
     * Producer to send data to broker.
     */
    private DefaultMQProducer producer;

    /**
     * Consumer to receive synchronize data from broker.
     */
    private DefaultMQPullConsumer consumer;

    /**
     * A queue to send or consume message.
     */
    private String topicName;

    /**
     * Used to convert key to byte[].
     */
    private Converter keyConverter;

    /**
     * Used to convert value to byte[].
     */
    private Converter valueConverter;

    private ExecutorService executors = Executors.newSingleThreadExecutor();

    private Map<MessageQueue, Long> messageQueues = new ConcurrentHashMap<>();
    private String workerId;
    private Map<MessageQueue, Long> suspendQueues = new ConcurrentHashMap<>();
    public BrokerBasedLog(ConnectConfig connectConfig,
                          String topicName,
                          String workId,
                          DataSynchronizerCallback<K, V> dataSynchronizerCallback,
                          Converter keyConverter,
                          Converter valueConverter) {

        this.topicName = topicName;
        this.dataSynchronizerCallback = dataSynchronizerCallback;
        this.producer = ConnectUtil.initDefaultMQProducer(connectConfig);
        this.producer.setProducerGroup(workId);
        this.consumer = ConnectUtil.initDefaultMQPullConsumer(connectConfig);
        this.consumer.setConsumerGroup(workId);
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.workerId = connectConfig.getWorkerId();
        this.prepare(connectConfig);
    }

    /**
     * Preparation before startup
     *
     * @param connectConfig
     */
    private void prepare(ConnectConfig connectConfig) {
        if (connectConfig.isAutoCreateGroupEnable()) {
            log.info("create sub group:"+consumer.getConsumerGroup());
            ConnectUtil.createSubGroup(connectConfig, consumer.getConsumerGroup());
        }
    }

    @Override
    public void start() {
        try {
            producer.start();
            consumer.registerMessageQueueListener(topicName, new MessageQueueListener() {
                @Override
                public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                    synchronized (messageQueues){
                        messageQueues.clear();
                        if (mqDivided!=null) {
                            mqDivided.stream().forEach(q->{
                                try{
                                    messageQueues.put(q,consumer.maxOffset(q));
                                }catch (MQClientException ex){
                                    log.error("",ex);
                                }
                            });
                        }
                    }
                }
            });
            consumer.start();
            final MessageListenerImpl messageListener = new MessageListenerImpl();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        boolean hasMessage = false;
                        synchronized (messageQueues){
                            for (MessageQueue queue : messageQueues.keySet()) {
                                final Long offSet = messageQueues.get(queue);
                                if (offSet!=null) {
                                    final Long nextPulltime = suspendQueues.get(queue);
                                    if (nextPulltime!=null && System.currentTimeMillis() < nextPulltime) {
                                        continue;
                                    }
                                    try {
                                        final PullResult pullResult = consumer.pull(queue,"",offSet,32);
                                        final List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
                                        if (msgFoundList!=null && pullResult.getPullStatus()==PullStatus.FOUND && msgFoundList.size()>0) {
                                            //只要有1个queue拉取到了消息，接下来会立刻拉第二次。否则的话就等2秒
                                            hasMessage = true;
                                            try{
                                                messageListener.consumeMessage(msgFoundList,null);
                                            }catch (Exception ex){
                                                log.error("consume broker based log failed :"+msgFoundList,ex);
                                            }finally {
                                                messageQueues.computeIfPresent(queue, (messageQueue, aLong) -> pullResult.getNextBeginOffset());
                                            }
                                        }else{
                                            suspendQueues.put(queue, System.currentTimeMillis() + 2000);
                                        }
                                    } catch (Exception e) {
                                        log.error("pull broker based log failed",e);
                                    }
                                }
                            }
                        }
                        try {
                            if (!hasMessage) {
                                Thread.sleep(2000);
                            }
                            hasMessage = false;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();

        } catch (MQClientException e) {
            log.error("Start error.", e);
        }
    }

    @Override
    public void stop() {
        producer.shutdown();
        consumer.shutdown();
    }

    @Override
    public void send(K key, V value) {

        try {
            byte[] messageBody = encodeKeyValue(key, value);
            if (messageBody.length > MAX_MESSAGE_SIZE) {
                log.error("Message size is greater than {} bytes, key: {}, value {}", MAX_MESSAGE_SIZE, key, value);
                return;
            }

            Map<String,Object> sendMap = new HashMap<>();
            sendMap.put(VALUE, messageBody);
            sendMap.put(WORKER_ID, workerId);

            producer.send(new Message(topicName, JSON.toJSONString(sendMap, SerializerFeature.WriteClassName).getBytes(StandardCharsets.UTF_8)), new SendCallback() {
                @Override public void onSuccess(org.apache.rocketmq.client.producer.SendResult result) {
                    log.info("Send SYS async message OK, msgId: {}  topic:{}", result.getMsgId(), topicName);
                }

                @Override public void onException(Throwable throwable) {
                    if (null != throwable) {
                        log.error("Send SYS async message Failed, error: {} and will retry until success", throwable);
                    }
                    executors.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Thread.sleep(1000);
                                BrokerBasedLog.this.send(key,value);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
            });
        } catch (Exception e) {
            log.error("BrokerBaseLog send SYS async message Failed.", e);
        }
    }

    private byte[] encodeKeyValue(K key, V value) throws Exception {

        byte[] keyByte = keyConverter.objectToByte(key);
        byte[] valueByte = valueConverter.objectToByte(value);
        Map<String, String> map = new HashMap<>();
        map.put(Base64.getEncoder().encodeToString(keyByte), Base64.getEncoder().encodeToString(valueByte));

        return JSON.toJSONString(map).getBytes("UTF-8");
    }

    private Map<K, V> decodeKeyValue(byte[] bytes) throws Exception {

        Map<K, V> resultMap = new HashMap<>();
        String rawString = new String(bytes, "UTF-8");
        Map<String, String> map = JSON.parseObject(rawString, Map.class);
        for (String key : map.keySet()) {
            K decodeKey = (K) keyConverter.byteToObject(Base64.getDecoder().decode(key));
            V decodeValue = (V) valueConverter.byteToObject(Base64.getDecoder().decode(map.get(key)));
            resultMap.put(decodeKey, decodeValue);
        }
        return resultMap;
    }

    class MessageListenerImpl implements MessageListenerConcurrently {
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList,
            ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : rmqMsgList) {
                byte[] bytes = messageExt.getBody();
                Map<K, V> map;
                try {
                    final HashMap<String,Object> parseMap = JSON.parseObject(bytes, HashMap.class);
                    final String workerId = (String) parseMap.get(WORKER_ID);
                    if (workerId.equals(BrokerBasedLog.this.workerId)) {
                        //收到了自己发出的消息
                        continue;
                    }
                    log.info("Received one message from {} ,msgId: {}, topic is {}",workerId, messageExt.getMsgId(), topicName);
                    map = decodeKeyValue((byte[]) parseMap.get(VALUE));
                } catch (Exception e) {
                    log.error("Decode message data error. message: {}, error info: {}", messageExt, e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                for (K key : map.keySet()) {
                    dataSynchronizerCallback.onCompletion(null, key, map.get(key));
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

}
