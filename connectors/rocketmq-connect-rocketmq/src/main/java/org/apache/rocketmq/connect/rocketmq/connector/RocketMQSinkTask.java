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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * rocketMQ消费位移的提交不在这里处理，而在框架里面，只要不抛异常出去就会提交最新位移
 */
public class RocketMQSinkTask extends SinkTask {
    private static Logger logger = LoggerFactory.getLogger(RocketMQSinkTask.class);

    private KafkaProducer kafkaProducer;
    private KeyValue config;
    private static final byte[] TRUE_BYTES = "true".getBytes(StandardCharsets.UTF_8);


    final AtomicReference<CountDownLatch> countDownLatchWrapper = new AtomicReference();
    final AtomicInteger count = new AtomicInteger(0);
    final AtomicReference<Exception> ex = new AtomicReference();
    final Callback callback = (metadata, exception) -> {
        try {
            count.incrementAndGet();
            ex.set(exception);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            countDownLatchWrapper.get().countDown();
        }
    };

    private Object[] payload;
    private Schema schema;
    private Field header;
    private String topic;
    private Field key;
    private Field value;
    private Map<String,String> headerPayLoad;

    /**
     * send to kafka 针对单线程0 GC的目标做了一定优化，不能在多线程环境下跑哈
     * @param sinkDataEntries
     */
    @Override
    public void put(Collection<SinkDataEntry> sinkDataEntries) {
        if (sinkDataEntries==null) {
            return;
        }

        //reset
        countDownLatchWrapper.set(new CountDownLatch(sinkDataEntries.size()));
        count.set(0);
        ex.set(null);

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

            headerPayLoad = (Map<String,String>) payload[header.getIndex()];
            if (Boolean.parseBoolean(headerPayLoad.get("by_connector"))) {
                //skip 这是从kafka拉下来然后同步到rocketMQ上面的消息，所以不能再从rocketMQ拉下来同步到kafka了。
                //同理，rocketMQ发送到kafka的消息也不能从kafka再拉下来发到rocketMQ了
                callback.onCompletion(null,null);
                continue;
            }

            topic = entry.getQueueName();
            key = schema.getField("key");
            value = schema.getField("value");

            List<RecordHeader> headerList = new ArrayList<>();
            for (Map.Entry<String, String> stringEntry : headerPayLoad.entrySet()) {
                headerList.add(new RecordHeader(stringEntry.getKey(),stringEntry.getValue().getBytes(StandardCharsets.UTF_8)));
            }
            headerList.add(new RecordHeader("by_connector", TRUE_BYTES));

            //注意如果key不为null时候的顺序性要求,这里key不空一定会被路由到同一个kafka的queue里面
            //这tm是异步啊,差点就写BUG了
            ProducerRecord record = new ProducerRecord(topic,null,payload[key.getIndex()], Bytes.wrap((byte[]) payload[value.getIndex()]),headerList);
            //logger.info("kafka msg send:"+new String((byte[]) payload[value.getIndex()]));
            kafkaProducer.send(record, callback);
        }
        try {
            countDownLatchWrapper.get().await();
        } catch (InterruptedException e) {
            logger.error("",e);
        }

        //有失败或者被中断，需要检查下刚才的消息是否全部处理成功，否则抛出异常
        int i = count.get();
        if (i != sinkDataEntries.size() || ex.get()!=null) {
            if (ex.get()!=null) {
                throw new RuntimeException("",ex.get());
            }else{
                throw new RuntimeException(String.format("sink was Interrupted but has not processed all sinkDataEntries: %s of %s",i,sinkDataEntries.size()));
            }
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
