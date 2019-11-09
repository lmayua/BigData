package com.ext.bigdata.flink.stream;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import akka.japi.tuple.Tuple3;

public class KafkaStreamDemo {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        
        String topicName = "";
        String groupId = "";
        String brokerList = "";// KAFKA brokerList
        String zkList = "";// zkList
        long startOffsetPart0 = 0L;
        long startOffsetPart1 = 0L;
        long startOffsetPart2 = 0L;
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerList);
        props.setProperty("zookeeper.connect", zkList);
        props.setProperty("group.id", groupId);
        
        // props.setProperty("enable.auto.commit", true);       //自动定期提交offset，setCommitOffsetsOnCheckpoints未设置为false，该配置无效
        // props.setProperty("auto.commit.interval.ms", 1000L);
        
        
        FlinkKafkaConsumer08 kafkaSource = new FlinkKafkaConsumer08<>(topicName, new SimpleStringSchema(), props);
        //kafkaSource.setCommitOffsetsOnCheckpoints(true);// 默认True，设置自动提交时，配置在properties中的offset的定时自动提交行为将会被忽略
                                                        // 当checkpoint处于completed的状态时，FlinkKafkaConsumer08会将offset存到checkpoint中。这保证了在Kafka brokers中的committed offset和checkpointed states中的offset保持一致
        //kafkaSource.setRuntimeContext(t);
        //kafkaSource.setStartFromEarliest(); // 从最早的消息开始消费。
        kafkaSource.setStartFromLatest(); //从最新的消息开始消费。
        //kafkaSource.setStartFromGroupOffsets();//采用consumer group的offset来作为起始位，这个offset从Kafka brokers(0.9以上版本)或 Zookeeper(Kafka 0.8)中获取。
                                                 //如果从Kafka brokers或者Zookeeper中找不到这个consumer group对应的partition的offset，那么auto.offset.reset这个配置就会被启用。
        
        /*Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();// 指定具体的某个offset作为某个partition的起始消费位置，未指定默认起始位置
        specificStartupOffsets.put(new KafkaTopicPartition(topicName, 0), startOffsetPart0);
        specificStartupOffsets.put(new KafkaTopicPartition(topicName, 1), startOffsetPart1);
        specificStartupOffsets.put(new KafkaTopicPartition(topicName, 2), startOffsetPart2);
        kafkaSource.setStartFromSpecificOffsets(specificStartupOffsets);*/
        
        // kafkaSource.initializeState(new FunctionInitializationContext() {});//从最后一个成功的checkpoint中获取各个partition的offset到restoredState中
        // kafkaSource.open(configuration);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000L); // checkpoint时间间隔/毫秒
        // checkpoint在外部存储，RETAIN_ON_CANCELLATION：当任务取消是，保留外部checkpoint；DELETE_ON_CANCELLATION：取消任务自动删除checkpoint
        //env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        String targetTopicName = "";
        FlinkKafkaProducer08<String> producer = new FlinkKafkaProducer08<String>(brokerList, targetTopicName, new SimpleStringSchema(Charset.forName("utf-8")));
        env.addSource(kafkaSource).map(new MapFunction<String, String>() {

            /**
             * serial ID
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String map(String inputMsg) throws Exception {
                System.out.println(inputMsg);
                return inputMsg;
            }
        })
        //.getExecutionConfig().setAutoWatermarkInterval(10 * 1000L)//可以自动周期生成watermark 
        .assignTimestampsAndWatermarks(new MyTimeExtractor(Time.seconds(10)))// watermark=最大时间戳-最大延时时间，小于watermark的数据都已达到，
        //.filter(new RichFilterFunction<T>() {})
        //.flatMap(new FlatMapFunction<T, O>() {})
        //.coGroup(otherStream) // 基本等同于join，coGroup中提供的apply方法，参数是Iterator[T1]与Iterator[2]这2种集合，对应SQL中类似于Table[T1]与Table[T2]
        //.join(otherStream)
        //.broadcast()
        //.writeAsText("E:/Work/tmp/flink_output/libra_out", WriteMode.OVERWRITE);

        .addSink(new SinkFunction() {
            @Override
            public void invoke(Object value) throws Exception {
                // TODO sink
            }
        })
        //.timeWindowAll(Time.seconds(20L))// batch schedule
        //.apply(new AllWindowFunction<IN, OUT, Window>() {})
        ;
        
        env.execute("flink-kafka-stream");
        /*String outputTopicName = "";
        String outputBrokerList = "";
        
        Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers", outputBrokerList);
        FlinkKafkaProducer08 producer = new FlinkKafkaProducer08<>(outputTopicName, new SimpleStringSchema(), producerConfig);*/
    }
    
    private static class MyTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>> {

        /**
         * serial ID
         */
        private static final long serialVersionUID = 1L;

        //在记录中看到的最大可见时间戳与要发射的水印的时间戳之间的固定时间间隔。
        private long lastEmittedWatermark = Long.MIN_VALUE;
        private long currentMaxTimestamp;
        private final long maxOutOfOrderness;
        private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public MyTimeExtractor(Time maxOutOfOrderness) {
            this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
            this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //保证了水印从不倒退。
            long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
            if (potentialWM >= lastEmittedWatermark) {
                lastEmittedWatermark = potentialWM;
            }
            System.out.println(String.format("call getCurrentWatermark======currentMaxTimestamp:%s  , lastEmittedWatermark:%s", format.format(new Date(currentMaxTimestamp)), format.format(new Date(lastEmittedWatermark))));
            return new Watermark(lastEmittedWatermark);
        }

        @Override
        public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
            //把第三个参数当做eventime
            long timestamp = element.t3();
            if (timestamp > currentMaxTimestamp) {
                currentMaxTimestamp = timestamp;
            }
            return timestamp;
        }
    }
}
