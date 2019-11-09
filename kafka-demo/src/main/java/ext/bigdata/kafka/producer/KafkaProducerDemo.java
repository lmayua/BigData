package ext.bigdata.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import ext.bigdata.kafka.util.SchemaUtils;

/**
 * KAFKA Demo
 * 版本0.10.x
 *
 * @author 17073580
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class KafkaProducerDemo {
    
    private String topicName;
    
    private KafkaProducer<String, byte[]> producer = null;
    
    public KafkaProducerDemo(String topicName) {
        if (null == producer){
            producer = new KafkaProducer<>(getConfigure());
        }
        this.topicName = topicName;
    }
    
    private Properties getConfigure(){
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
        Properties pro = new Properties();
        //props.put("client.id", "client_id");
        pro.put("acks", "all");
        pro.put("retries", 0);
        //props.put("batch.size", 16384);
        pro.put("linger.ms", 1);
        //props.put("buffer.memory", 33554432);
        //props.put("partitioner.class", "com.suning.erp.ts.kafkats.RandomPartitioner"); // 自定义分区器，继承org.apache.kafka.clients.producer.Partitioner
        // TODO
        String brokerList = "";
        pro.setProperty("bootstrap.servers", brokerList);
        pro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return pro;
    }
    
    /**
     * 发送消息
     *
     * @param key 用于分区器计算发送到具体分区值
     * @param value 消息
     * @param callback 回调方法
     */
    public void sendMsg(String key, String value, Callback callback){
        Future<RecordMetadata> metaFuture = producer.send(new ProducerRecord<String, byte[]>(this.topicName, null, System.currentTimeMillis(), key, value.getBytes()), callback);
        try {
            System.out.println(metaFuture.get().toString());
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        // 返回结果为Future<RecordMetadata>，Future.get()接口是阻塞方式，0.8.2版本之后都为异步方式，该方式可以实现同步，但会影响性能
        // 异步方式想要获取其返回结果，可以使用传入回调类操作，一般只是做信息记录
    }
    
    public void sendMsg(String key, String value){
        sendMsg(key, value, null);
    }
    
    /**
     * 发送消息
     * 
     * @param msg
     */
    public void sendMsg(String msg){
        sendMsg(null, msg, null);
    }
    
    /**
     * Producer回调类
     */
    class ProducerCallback implements Callback{
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
            
            if (null != exception){
                System.out.println(exception.getMessage());
                System.out.println(exception.getStackTrace());
            }
        }
    }
    
    public static void main(String[] args) {
       /* KafkaProducerDemo demo = new KafkaProducerDemo("sies_bhvr_basic");
        String msg = "{\"proDate\":\"2019-03-28\","
                    + "\"applyScore\":\"\","
                    + "\"objectPhone\":\"\","
                    + "\"applyerName\":\"\","
                    + "\"haviorNum\":\"HRZP2019032800000001\","
                    + "\"ladderNum\":\"\","
                    + "\"applyDescribe\":\"\","
                    + "\"otherInstr\":\"\","
                    + "\"objectName\":\"洪海波\","
                    + "\"applyerId\":\"\","
                    + "\"tranDate\":\"2019-03-28\","
                    + "\"scoreNum\":\"HRRS001\","
                    + "\"applyCode\":\"\","
                    + "\"applyDate\":\"\","
                    + "\"objectID\":\"16060566\"}";
        Callback callback = demo.new ProducerCallback();
        demo.sendMsg("10", msg, callback);*/
        
        KafkaProducerDemo demo = new KafkaProducerDemo("sies_bhvr_detail");
        String msg = "{\"haviorNum\":\"HRZP2019032800000001\","
                    + "\"categoryName\":\"\","
                    + "\"categoryDescribe\":\"\","
                    + "\"className\":\"\","
                    + "\"classDescribe\":\"\","
                    + "\"scoreNum\":\"HRRS001\","
                    + "\"basisTitle\":\"\","
                    + "\"scoreDescribe\":\"\","
                    + "\"scoreType\":\"\","
                    + "\"ladderNum\":\"\","
                    + "\"objectID\":\"16060566\","
                    + "\"objectName\":\"洪海波\","
                    + "\"objectPhone\":\"\","
                    + "\"proDate\":\"2019-04-01 00:00:00\","
                    + "\"otherInstr\":\"\","
                    + "\"getScore\":\"-10\","
                    + "\"applyerId\":\"\","
                    + "\"applyerName\":\"\","
                    + "\"applyDate\":\"\","
                    + "\"applyCode\":\"\","
                    + "\"applyDescribe\":\"\","
                    + "\"sourcelType\":\"\","
                    + "\"integralType\":\"\","
                    + "\"tranDate\":\"2019-04-01 00:00:00\"}";
        Schema schema = SchemaUtils.getSchema("conf/schema");
        
        GenericRecord record = new GenericData.Record(schema);
        record.put("haviorNum","HRZP2019032800000001");
        record.put("categoryName","");
        record.put("categoryDescribe","");
        record.put("className","");
        record.put("classDescribe","");
        record.put("scoreNum","HRRS001");
        record.put("basisTitle","");
        record.put("scoreDescribe","");
        record.put("scoreType","");
        record.put("ladderNum","");
        record.put("objectID","16060566");
        record.put("objectName","洪海波");
        record.put("objectPhone","");
        record.put("proDate","2019-04-01 00:00:00");
        record.put("otherInstr","");
        record.put("getScore",-10);
        record.put("applyerId","");
        record.put("applyerName","");
        record.put("applyDate","");
        record.put("applyCode","");
        record.put("applyDescribe","");
        record.put("sourcelType","");
        record.put("integralType","");
        record.put("tranDate","2019-04-01 00:00:00");
        
        byte[] bytes = SchemaUtils.getSchemaByte(record, schema);
        Callback callback = demo.new ProducerCallback();
        demo.sendMsg("0", new String(bytes), callback);
    }
    
    /**
     * 自定义分区器，一般情况下KAFKA自带分区器能够满足大多数业务需求，不需要自定义
     */
    /*class RandomPartitioner implements Partitioner{

        @Override
        public void configure(Map<String, ?> map) {
        }

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            List<PartitionInfo> partintionInfoList =  cluster.partitionsForTopic(topic);
            cluster.availablePartitionsForTopic(topic);
            return 0;
        }

        @Override
        public void close() {
        }
        
    }*/
}
