package com.ext.bigdata.flink.stream;

import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import scala.runtime.RichBoolean;

public class SocketStreamDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource dss = env.socketTextStream("127.0.0.1", 24);
        
        /*dss.map(new RichMapFunction<IN, OUT>() {
        })
        .filter(new RichFilterFunction<T>() {
        })
        //.join(otherStream)
        //.coGroup(otherStream)
        .keyBy(0)
        .timeWindowAll(Time.minutes(5L))
        .aggregate(new RichAggregateFunction<IN, ACC, OUT>() {
        })
        .addSink(new RichSinkFunction<IN>() {
            
        });
        env.execute("socket-demo-job");*/
    }
}
