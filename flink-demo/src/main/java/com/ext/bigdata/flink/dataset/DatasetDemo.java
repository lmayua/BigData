package com.ext.bigdata.flink.dataset;

import java.util.Map;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;

public class DatasetDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        DataSet<String> ds = env.readTextFile("E:/Work/tmp/test_data/da-bi_da_oms_detail-consumer.txt");
        
        IntCounter counter = new IntCounter(0);
        
        ds.mapPartition(new RichMapPartitionFunction<String, String>(){
            
            private static final long serialVersionUID = 1L;

            @Override
            public void mapPartition(Iterable<String> inputs, Collector<String> out) throws Exception {
                for (String input : inputs){
                    Map<String, Object> inputMap = JSONObject.parseObject(input);
                    System.out.println(inputMap.get("orderItemId"));
                    out.collect(input);
                }
            }
        })
        //.distinct() 
        //.aggregate(Aggregations.SUM, 0) // 只支持SUM、MIN、MAX
        .writeAsText("E:/Work/tmp/test_data/dataset-demo");
        //.count();
        //System.out.println("-->> count: " + count);
        
        
        env.execute("dataset-demo-job");
    }
}
