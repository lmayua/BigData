package ext.bigdata.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.security.auth.callback.Callback;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * kafka version 0.10
 */
public class KafkaConsumerDemo {
    
    private KafkaConsumer<String, String> consumer = null;
    private List<String> topicList;
    
    private long minBatchSize = 500L;
    
    
    public KafkaConsumerDemo(List<String> topicList) {
        
        consumer = new KafkaConsumer<>(getConfigure());
        this.topicList = topicList;
    }
    
    private Properties getConfigure(){
        Properties props = new Properties();
        String servers = "";
        props.put("bootstrap.servers", servers);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    
    public void consumerMsg(ConsumerCallBack<String> callback) {
        consumer.subscribe(this.topicList);// 0.9新特性，支持消息订阅
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100); // 
            for (ConsumerRecord<String, String> record : records) {
                callback.dealMsg(record.value());
                System.out.println("checksum: " + record.key());
                System.out.println("checksum: " + record.value());
                System.out.println("checksum: " + record.checksum());
                System.out.println("offset: " + record.offset());
                System.out.println("partition: " + record.partition());
                System.out.println("serializedKeySize: " + record.serializedKeySize());
                System.out.println("serializedValueSize: " + record.serializedValueSize());
                System.out.println("timestamp: " + record.timestamp());
                System.out.println("topic: " + record.topic());
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                consumer.commitSync();// 同步方式提交，阻塞直至成功或者异常 
                                      // 只支持kafka存储的offset方式，其他外部存储维护offset则不适合该API
                buffer.clear();
            }
        }
    }
    
    
    interface ConsumerCallBack<T> extends Callback{
        public T dealMsg(String msg);
    }
}
