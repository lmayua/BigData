package ext.bigdata.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;

public class Test {
    public static void main(String[] args) {
        Properties props = new Properties();
        /*
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("partitioner.class", "ext.bigdata.kafka.RandomPartitioner");
        properties.put("metadata.broker.list", kafkaBrokerProperties);
        */
        
        /*
                配置项：
        2,1,配置项client.id，用于标记client端的一个编码值，默认值为producer-1。在同一个进程内，多个client端时，如果没有指定，默认根据1这个值向后增加。
        2,2,配置项partitioner.class，配置用于producer写入数据时用于计算这条数据对应的partition的分配算子实例，这个实例必须是的Partitioner实现。实例初始化时会调用configure函数把配置文件传入进去，用于实例生成时使用，默认情况下分区算子是DefaultPartitioner。这个默认算子根据当前的key值进行murmur2hash并与对应的topic的个数于模，如果key为null时，根据一个自增的integer的值与partition的个数取模.
        2,3,配置项retry.backoff.ms，用于在向broker发送数据失败后的重试间隔时间，默认值为100ms
        2,4,配置项metadata.max.age.ms，用于配置每个producer端缓存topic的metadata的过期时间，默认值为5分钟。配置上面的2,3，与2,4的配置，生成一个Metadata实例。
        2,5,配置项max.request.size，用于配置每次producer请求的最大的字节数，默认值为1MB。
        2,6,配置项buffer.memory，用于配置producer端等待向server发送的数据的缓冲区的大小，默认值为32MB。
        2,7,配置项compression.type，默认值none，用于配置数据的压缩算法，默认为不压缩，可配置的值为none,gzip,snappy,lz4。
        2,8,配置项max.block.ms，用于配置send数据或partitionFor函数得到对应的leader时，最大的等待时间，默认值为60秒。
        2,9,配置项request.timeout.ms，用于配置socket请求的最大超时时间，默认值为30秒。
        
        */
        
        props.put("bootstrap.servers", "kafkasit02broker01.cnsuning.com:9092,kafkasit02broker02.cnsuning.com:9092,kafkasit02broker03.cnsuning.com:9092");
        //props.put("client.id", "client_id");
        props.put("acks", "all");
        props.put("retries", 0);
        //props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        //props.put("buffer.memory", 33554432);
        //props.put("partitioner.class", "com.suning.erp.ts.kafkats.RandomPartitioner"); // 自定义分区器，继承org.apache.kafka.clients.producer.Partitioner
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        String topic = "libra_out_da";
        String key = null;
        String value = "test msg";
        // Integer_partitionNo: 分区序号，一般默认不传（特别的自定义分区器，不传或者null）
        // Long_timestamp: 时间戳，用户自定义时间戳，一般为数据时间
        // 
        
        producer.send(new ProducerRecord<String, String>(topic, null, System.currentTimeMillis(), key, value), new ProducerCallback());
        producer.close();
    }
    
    
    
    static class ProducerCallback implements Callback{
        @Override
        public void onCompletion(RecordMetadata recordmetadata, Exception exception) {
            if (null != recordmetadata){
                System.out.println("checksum: " + recordmetadata.checksum());
                System.out.println("offset: " + recordmetadata.offset());
                System.out.println("partition: " + recordmetadata.partition());
                System.out.println("serializedKeySize: " + recordmetadata.serializedKeySize());
                System.out.println("serializedValueSize: " + recordmetadata.serializedValueSize());
                System.out.println("timestamp: " + recordmetadata.timestamp());
                System.out.println("topic: " + recordmetadata.topic());
            }
        }
    }
    
    static class RandomPartitioner implements Partitioner{

        @Override
        public void configure(Map<String, ?> map) {
        }

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return 0;
        }

        @Override
        public void close() {
        }
        
    }
    
}
