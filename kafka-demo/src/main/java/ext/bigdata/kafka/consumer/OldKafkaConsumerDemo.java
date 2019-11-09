package ext.bigdata.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 *
 */
public class OldKafkaConsumerDemo {

    /**
     * topic
     */
    private String topic;

    /**
     * group ID
     */
    private String groupId;

    /**
     * connector
     */
    private ConsumerConnector connector;

    /**
     * <default constructor>
     */
    public OldKafkaConsumerDemo(String topic, String groupId) {
        this.topic = topic;
        this.groupId = groupId;
        this.connector = createConsumerConnector();
    }

    /**
     * 创建消费连接
     */
    private ConsumerConnector createConsumerConnector() {
        Properties properties = new Properties();
        String connectIps = "";
        properties.put("zookeeper.connect", connectIps);// sit
        properties.put("group.id", this.groupId);
        properties.put("socket.timeout.ms", "30000");
        properties.put("zookeeper.connection.timeout.ms", "30000");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    /**
     * 消费方法  => High API
     */
    public void consumeMsg(ConsumerCallBack<String> callback) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(this.topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = this.connector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> kafkaStream = consumerMap.get(this.topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
        while (iterator.hasNext()) {
            callback.dealMsg(new String(iterator.next().message()));
        }
    }

    /**
     * call back method for deal message
     */
    interface ConsumerCallBack<T> {
        public T dealMsg(String msg);
    }

    /**
     * test main method
     */
    public static void main(String[] args) {
        //KafkaConsumer consumer = new KafkaConsumer("bi_dfp_oms_serv_unit", "da-bi_dfp_oms_serv_unit-consumer");
        OldKafkaConsumerDemo consumer = new OldKafkaConsumerDemo("sies_bhvr_detail", "SIES_sies_bhvr_detail");
        consumer.consumeMsg(new ConsumerCallBack<String>() {
            @Override
            public String dealMsg(String msg) {
                System.out.println(msg);
                return msg;
            }
        });
    }
}
