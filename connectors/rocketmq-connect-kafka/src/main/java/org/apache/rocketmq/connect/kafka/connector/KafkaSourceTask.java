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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.kafka.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.connector.api.source.SourceTask;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.rocketmq.connect.kafka.config.ConfigDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 *  连接器实例属于逻辑概念，其负责维护特定数据系统的相关配置，比如链接地址、需要同步哪些数据等信息；
 *  在connector 实例被启动后，connector可以根据配置信息，对解析任务进行拆分，分配出task。这么做的目的是为了提高并行度，提升处理效率
 *  注意因为类加载器的缘故，基本上这里都不允许异步的方法
 *
 *  从kafka中poll下来数据后，需要提交kafka的位移，而这里使用的是自动提交位移机制 enable.auto.commit = true
 */
public class KafkaSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(KafkaSourceTask.class);
    private KafkaConsumer<ByteBuffer, ByteBuffer> consumer;
    private KeyValue config;
    private List<String> topicList;
    //这currentTPList 线程不安全，重平衡发生时候是并发的
    private final List<TopicPartition> currentTPList = new CopyOnWriteArrayList<>();
    private static final byte[] TRUE_BYTES = "true".getBytes(StandardCharsets.UTF_8);
    //启动定时任务提交位移
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private MyOffsetCommitCallback commitCallback =  new MyOffsetCommitCallback();

    private ReentrantReadWriteLock partitionLock = new ReentrantReadWriteLock(true);
    @Override
    public Collection<SourceDataEntry> poll() {
        try {

            ArrayList<SourceDataEntry> entries = new ArrayList<>();
            //这应该不用加锁
            ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(500);

            for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
                String topic_partition = record.topic() + "-" + record.partition();
                log.info("Received {} record: {} ", topic_partition, record);
                //header
                Headers headers = record.headers();
                Map<String, byte[]> map = new HashMap<>(4);
                boolean run = true;
                if (headers!=null) {
                    for (Header header : headers) {
                        String key = header.key();
                        byte[] value = header.value();

                        //skip this message
                        if ("by_connector".equals(key) && Boolean.parseBoolean(new String(value))) {
                            run = false;
                            break;
                        }
                        map.put(key,value);
                    }
                }
                if (!run) {
                    continue;
                }

                map.put("by_connector",TRUE_BYTES);

                Schema schema = new Schema();
                List<Field> fields = new ArrayList<>();
                fields.add(new Field(0, "key", FieldType.BYTES));
                fields.add(new Field(1, "value", FieldType.BYTES));
                fields.add(new Field(2, "header", FieldType.MAP));
                schema.setName(record.topic());
                schema.setFields(fields);
                schema.setDataSource(record.topic());

                ByteBuffer sourcePartition = ByteBuffer.wrap(topic_partition.getBytes());
                ByteBuffer sourcePosition = ByteBuffer.wrap(String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8));

                //把kafka record构建为dataEntryBuilder
                DataEntryBuilder dataEntryBuilder = new DataEntryBuilder(schema);
                dataEntryBuilder.entryType(EntryType.CREATE);
                dataEntryBuilder.queue(record.topic()); //queueName will be set to RocketMQ topic by runtime
                dataEntryBuilder.timestamp(System.currentTimeMillis());

                //key
                if (record.key() != null) {
                    dataEntryBuilder.putFiled("key", record.key().array());
                } else {
                    dataEntryBuilder.putFiled("key", null);
                }

                //value
                dataEntryBuilder.putFiled("value", record.value().array());

                //header
                dataEntryBuilder.putFiled("header",map);

                //sourcePartition = topic_partition ,即topic+分区
                //sourcePosition = record.offset()
                SourceDataEntry entry = dataEntryBuilder.buildSourceDataEntry(sourcePartition, sourcePosition);
                entries.add(entry);
            }

            log.debug("poll return entries size {} ", entries.size());
            return entries;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("poll exception {}", e);
        }
        return null;
    }

    @Override
    public void start(KeyValue taskConfig) {
        log.info("source task start enter");
        this.topicList = new ArrayList<>();
        this.config = taskConfig;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.config.getString(ConfigDefine.BOOTSTRAP_SERVER));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.config.getString(ConfigDefine.GROUP_ID));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteBufferDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteBufferDeserializer");

        this.consumer = new KafkaConsumer<>(props);

        String topics = this.config.getString(ConfigDefine.TOPICS);
        for (String topic : topics.split(",")) {
            if (!topic.isEmpty()) {
                topicList.add(topic);
            }
        }

        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                synchronized (currentTPList){
                    commitOffset(currentTPList, false);
                }
            }
        },3000,1000, TimeUnit.MILLISECONDS);

        consumer.subscribe(topicList, new MyRebalanceListener());
        log.info("source task subscribe topicList {}", topicList);
    }

    @Override
    public void stop() {
        log.info("source task stop enter");
        try{
            scheduledExecutorService.shutdown();
            scheduledExecutorService.awaitTermination(5 * 1000 * 60, TimeUnit.MILLISECONDS);
        }catch (Exception ex){
            log.warn("",ex);
        }
        try {
            synchronized (currentTPList){
                commitOffset(currentTPList, true);
            }
            consumer.wakeup(); // wakeup poll in other thread
            consumer.close();
        } catch (Exception e) {
            log.warn("{} consumer {} close exception {}", this, consumer, e);
        }
    }

    @Override
    public void pause() {
        log.info("source task pause ...");
        consumer.pause(currentTPList);
    }

    @Override
    public void resume() {
        log.info("source task resume ...");
        consumer.resume(currentTPList);
    }

    public String toString() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String pid = name.split("@")[0];
        return "KafkaSourceTask-PID[" + pid + "]-" + Thread.currentThread().toString();
    }

    public static TopicPartition getTopicPartition(ByteBuffer buffer)
    {
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;
        try
        {
            charset = Charset.forName("UTF-8");
            decoder = charset.newDecoder();
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            String topic_partition = charBuffer.toString();
            int index = topic_partition.lastIndexOf('-');
            if (index != -1 && index > 1) {
                String topic = topic_partition.substring(0, index - 1);
                int partition = Integer.parseInt(topic_partition.substring(index + 1));
                return new TopicPartition(topic, partition);
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            log.warn("getString Exception {}", ex);
        }
        return null;
    }

    /**
     * 这里提交kafka偏移量，只提交发rocketMQ发送成功收到回调success的那些消息，也就是说消费位移是context.positionStorageReader()读出来的
     * 其实就是 positionManagementService.getPositionTable().get(partition) ，而这个positionManagementService就是在rocketMQ消息发出去收到回调才会put进去
     * 而且这个提交位移好像不是定时提交，只在stop和发生重平衡才会提交位移。这个问题不是会很大吗。不会,已经 ENABLE_AUTO_COMMIT_CONFIG 也就是自动你提交参数打开了
     * TODO kafka的自动位移提交应该不行，因为未必发送到rocketMQ是成功的，所以消费位移也得以rocketMQ发送出去的消息的位移为准
     * @param tpList
     * @param sync 是否同步执行
     */
    private void commitOffset(Collection<TopicPartition> tpList, boolean sync) {
        if(tpList == null || tpList.isEmpty())
            return;

        List<ByteBuffer> topic_partition_list = new ArrayList<>();
        for (TopicPartition tp : tpList) {
            //如果重平衡正好发生，此时正在迭代。是不是有问题？所以改成了copyOnWriteArrayList
            topic_partition_list.add(ByteBuffer.wrap((tp.topic() + "-" + tp.partition()).getBytes()));
        }

        Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
        Map<ByteBuffer, ByteBuffer> topic_position_map = context.positionStorageReader().getPositions(topic_partition_list);
        if (topic_position_map==null || topic_position_map.size()==0) {
            return;
        }
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : topic_position_map.entrySet()) {
            TopicPartition tp = getTopicPartition(entry.getKey());
            if (tp != null && tpList.contains(tp)) {
                //positionStorage store more than this task's topic and partition
                try {
                    long local_offset = Long.parseLong(new String(entry.getValue().array()));
                    commitOffsets.put(tp, new OffsetAndMetadata(local_offset));
                } catch (Exception e) {
                    log.warn("commitOffset get local offset exception {}", e);
                }
            }
        }

        if (!commitOffsets.isEmpty()) {
            if (sync) {
                consumer.commitSync(commitOffsets);
                commitCallback.onComplete(commitOffsets,null);
            } else {
                consumer.commitAsync(commitOffsets,commitCallback);
            }
        }
    }

    private class MyOffsetCommitCallback implements OffsetCommitCallback {

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
            if (e != null) {
                log.warn("commit async excepiton", e);
                map.entrySet().stream().forEach((Map.Entry<TopicPartition, OffsetAndMetadata> entry) -> {
                    log.warn("commit exception, TopicPartition: {} offset: {}", entry.getKey().toString(), entry.getValue().offset());
                });
                return;
            }
        }
    }

    private class MyRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            synchronized (currentTPList){
                currentTPList.clear();
                for (TopicPartition tp : partitions) {
                    log.info("onPartitionsAssigned TopicPartition {}", tp);
                    currentTPList.add(tp);
                }
            }
        }

        /**
         * 当kafka分区不再分配给自己的时候，需要提交kafka的消费位移
         * @param partitions
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("onPartitionsRevoked {} Partitions revoked", KafkaSourceTask.this);
            try {
                commitOffset(partitions, false);
            } catch (Exception e) {
                log.warn("onPartitionsRevoked exception", e);
            }
        }
    }
}
