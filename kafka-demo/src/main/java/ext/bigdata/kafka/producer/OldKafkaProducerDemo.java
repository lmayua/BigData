package ext.bigdata.kafka.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import ext.bigdata.kafka.util.SchemaUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class OldKafkaProducerDemo {
    /*public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("partitioner.class", "com.suning.da.common.RandomPartitioner");
        properties.put("metadata.broker.list", "${brokerList}");
        ProducerConfig config = new ProducerConfig(properties);
        Producer producer = new Producer<String, String>(config);
    }*/

    private final kafka.javaapi.producer.Producer<String, byte[]> producer;
    private final String topic;

    private final Properties props = new Properties();

    public OldKafkaProducerDemo(String topic) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        String brokerList = "";
        props.put("metadata.broker.list", brokerList);// sit
        producer = new kafka.javaapi.producer.Producer<String, byte[]>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void send(String file) throws IOException {
        File file1 = new File(file);
        if (file1.isDirectory()) {
            File[] files = file1.listFiles();
            for (File f : files) {
                BufferedReader br = new BufferedReader(new FileReader(f));
                String line = "";
                while ((line = br.readLine()) != null) {
                    producer.send(new KeyedMessage<String, byte[]>(topic, line.getBytes()));
                }
                br.close();
            }
        } else {
            //while (true) {
            BufferedReader br = new BufferedReader(new FileReader(new File(file)));
            String line = "";
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                producer.send(new KeyedMessage<String, byte[]>(topic, line.getBytes()));
            }
            br.close();
            //}
        }
    }

    public void sendMsg(byte[] msg) {
        producer.send(new KeyedMessage<String, byte[]>(topic, "0", msg));
    }

    /*public static void main(final String[] args) {
        if (args.length != 3) {
            System.out.println("please input <topic> <jsonfile>");
            System.exit(-1);
        }
    
        for (int i = 0; i < Integer.parseInt(args[2]); i++) {
            new Thread(new Runnable() {
                public void run() {
                    try {
                        OldKafkaProducerDemo p = new OldKafkaProducerDemo(args[0]);
                        p.send(args[1]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }*/
    public static void main(String[] args) {
        OldKafkaProducerDemo demo = new OldKafkaProducerDemo("sies_bhvr_detail");
        Schema schema = SchemaUtils.getSchema("conf/schema");

        GenericRecord record = new GenericData.Record(schema);
        record.put("haviorNum", "HRZP2019032800000001");
        record.put("categoryName", "");
        record.put("categoryDescribe", "");
        record.put("className", "");
        record.put("classDescribe", "");
        record.put("scoreNum", "HRRS001");
        record.put("basisTitle", "");
        record.put("scoreDescribe", "");
        record.put("scoreType", "");
        record.put("ladderNum", "");
        record.put("objectID", "16060566");
        record.put("objectName", "洪海波");
        record.put("objectPhone", "");
        record.put("proDate", "2019-04-01 00:00:00");
        record.put("otherInstr", "");
        record.put("getScore", -10);
        record.put("applyerId", "");
        record.put("applyerName", "");
        record.put("applyDate", "");
        record.put("applyCode", "");
        record.put("applyDescribe", "");
        record.put("sourcelType", "");
        record.put("integralType", "");
        record.put("tranDate", "2019-04-01 00:00:00");

        byte[] bytes = SchemaUtils.getSchemaByte(record, schema);
        
        demo.sendMsg(bytes);
    }
}
