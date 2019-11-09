package ext.bigdata.spark.stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StructuredStreamingDemo {
    public static void main(String[] args) {
        String topicName = "libra_out_da";
        String groupId = "da-libra_out_da-consumer";
        String brokerList = "kafkasit02broker01.cnsuning.com:9092,kafkasit02broker02.cnsuning.com:9092,kafkasit02broker03.cnsuning.com:9092";
        String zkList = "kafkasit02zk01.cnsuning.com:2181,kafkasit02zk02.cnsuning.com:2181,kafkasit02zk03.cnsuning.com:2181";

        SparkSession session = SparkSession.builder().master("local").appName("local-app").getOrCreate();
        //session.read().csv(...)

        Dataset<Row> ds = session.readStream().format("kafka").option("kafka.bootstrap.servers", brokerList)
                .option("subscribe", topicName).load();
        
    }
}
