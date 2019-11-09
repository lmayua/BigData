package ext.bigdata.kafka.producer;

public class Test {
    public static void main(String[] args) {
        KafkaProducerDemo demo = new KafkaProducerDemo("libra_csi_index_count_output_topic_unit");
        demo.sendMsg("t1", "test message 1");
    }
}
