package org.apache.rocketmq.connect.rocketmq.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.common.QueueMetaData;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SinkDataEntry;
import io.openmessaging.connector.api.sink.SinkTask;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.Bytes;
import org.apache.rocketmq.connect.rocketmq.config.ConfigDefine;
import org.apache.rocketmq.connect.rocketmq.util.GsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * rocketMQ消费位移的提交不在这里处理，而在框架里面，只要不抛异常出去就会提交最新位移
 */
public class RocketMQSinkTask extends SinkTask {
    private static Logger logger = LoggerFactory.getLogger(RocketMQSinkTask.class);
    private static Logger logger4SinkMsg = LoggerFactory.getLogger("logger4SinkMsg");

    private KafkaProducer kafkaProducer;
    private KeyValue config;
    private static final byte[] TRUE_BYTES = "true".getBytes(StandardCharsets.UTF_8);

    /**
     * @param sinkDataEntries
     */
    @Override
    public void put(Collection<SinkDataEntry> sinkDataEntries) {
        if (sinkDataEntries==null) {
            return;
        }

        Object[] payload;
        Schema schema;
        Field header;
        String topic;
        Field key;
        Field value;
        Map<String,String> headerPayLoad;

        final CountDownLatch countDownLatch = new CountDownLatch(sinkDataEntries.size());
        AtomicBoolean successAll = new AtomicBoolean(true);

        for (SinkDataEntry entry : sinkDataEntries) {
            //reset
            payload = null;
            schema = null;
            header = null;
            topic = null;
            key = null;
            value = null;

            payload = entry.getPayload();
            schema = entry.getSchema();
            header = schema.getField("header");

            Callback callback = (metadata, exception) -> {
                try{
                    if (exception!=null) {
                        successAll.set(false);
                        logger4SinkMsg.error(String.format("msg send to kafka failed async in callBack entry:%s",entry),exception);
                    }
                }finally {
                    countDownLatch.countDown();
                }
            };

            headerPayLoad = (Map<String,String>) payload[header.getIndex()];
            if (Boolean.parseBoolean(headerPayLoad.get("by_connector"))) {
                //skip 这是从kafka拉下来然后同步到rocketMQ上面的消息，所以不能再从rocketMQ拉下来同步到kafka了。
                //同理，rocketMQ发送到kafka的消息也不能从kafka再拉下来发到rocketMQ了
                callback.onCompletion(null,null);
                continue;
            }

            topic = config.getString(ConfigDefine.KAFKA_TOPIC);
            key = schema.getField("key");
            value = schema.getField("value");

            List<RecordHeader> headerList = new ArrayList<>();
            //注释的原因是，rocketMQ没有任何Header字段需要同步到kafka,其中Delay没用，而Keys会额外处理，Tags这里才处理。除此之外不兼容其他字段
            for (Map.Entry<String, String> stringEntry : headerPayLoad.entrySet()) {
                if("TAGS".equals(stringEntry.getKey())){
                    headerList.add(new RecordHeader(stringEntry.getKey(),stringEntry.getValue().getBytes(StandardCharsets.UTF_8)));
                }else if("FullLinkContext".equals(stringEntry.getKey())){
                    headerList.add(new RecordHeader(stringEntry.getKey(),stringEntry.getValue().getBytes(StandardCharsets.UTF_8)));
                }
            }
            headerList.add(new RecordHeader("by_connector", TRUE_BYTES));

            //注意如果key不为null时候的顺序性要求,这里key不空一定会被路由到同一个kafka的queue里面
            //这tm是异步啊,差点就写BUG了
            byte[] byteValue = (byte[]) payload[value.getIndex()];
            List<Object> list = GsonUtil.fromJson(new String(byteValue), List.class);

            ProducerRecord record = new ProducerRecord(topic, null, payload[key.getIndex()], Bytes.wrap(list.get(0).toString().getBytes()), headerList);
            if (payload[key.getIndex()]!=null) {
                try {
                    final Future<RecordMetadata> send = kafkaProducer.send(record);
                    final RecordMetadata recordMetadata = send.get(5000, TimeUnit.MILLISECONDS);
                    callback.onCompletion(recordMetadata,null);
                } catch (Exception e) {
                    logger4SinkMsg.error(String.format("send sync to kafka failed :%s", entry));
                    //只要任何一条SYNC消息报错，整个直接异常抛出去了，也就不需要countLatch-1了
                    throw new RuntimeException("send sync to kafka failed :"+record);
                }
            }else{
                //异步
                kafkaProducer.send(record,callback);
            }
        }
        try {
            final boolean await = countDownLatch.await(10 * 60 * 1000, TimeUnit.MILLISECONDS);
            if (!await) {
                //超时了
                throw new RuntimeException("countDownLatch time out when send msg to kafka and will consume rocketMQ message again");
            }
            if (!successAll.get()) {
                throw new RuntimeException("msg async send to kafka call back response failed and will consume rocketMQ message again");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("countDownLatch was interrupted when send msg to kafka and will consume rocketMQ message again");
        }
    }

    @Override
    public void commit(Map<QueueMetaData, Long> offsets) {
        //do nothing 不要自己提交 交给框架，connect-runtime运行连接到的rocketMQ集群就是sink的数据源，所以交给框架处理位移
    }

    @Override
    public void start(KeyValue config) {
        this.config = config;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(ConfigDefine.BOOTSTRAP_SERVER));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "2");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //value无需转换,直接字节数组发送
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesSerializer");

        kafkaProducer = new KafkaProducer(props);
        logger.info(String.format("rocketmq sink task start success with bootStrapServers:%s groupId:%s",props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),props.get(ConsumerConfig.GROUP_ID_CONFIG)));
    }

    @Override
    public void stop() {
        logger.info("RocketMQSinkTask stop...");
        //stop前提交一次位移 但我一个sink端没啥好提交的啊,rocketMQ本身的位移框架应该会处理啊。但是注意可能还有遗留消息刚拉下来没来得及发送出去
        if (kafkaProducer!=null) {
            kafkaProducer.close();
        }
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }
}
