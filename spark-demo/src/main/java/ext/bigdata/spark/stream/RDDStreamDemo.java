package ext.bigdata.spark.stream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class RDDStreamDemo {
    public static void main(String[] args) {
        
        String topicName = "libra_out_da";
        String groupId = "da-libra_out_da-consumer";
        String brokerList = "kafkasit02broker01.cnsuning.com:9092,kafkasit02broker02.cnsuning.com:9092,kafkasit02broker03.cnsuning.com:9092";
        String zkList = "kafkasit02zk01.cnsuning.com:2181,kafkasit02zk02.cnsuning.com:2181,kafkasit02zk03.cnsuning.com:2181";
        
        // method-1
        /*SparkConf conf = new SparkConf();
        //conf.set("spark.default.parallelism", "4");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(20L));
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        int partitions = 6;
        topicMap.put(topicName, partitions);
        JavaPairInputDStream<String, String> messages = KafkaUtils.createStream(context, zkList, topicName, topicMap);*/
        
        // method-2
        SparkConf sc = new SparkConf();
        //conf.set("spark.default.parallelism", "4");
        sc.setAppName("local-app").setMaster("local");
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(20L));
        //jsc.getOrCreate("${checkpoint_path}", new Function0);jsc.start();
        //jsc.checkpoint("/xxx/xxx");
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokerList);
        Set<String> topicSet = new HashSet<String>();
        topicSet.add(topicName);
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jsc, String.class, String.class,StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);
    }
}
