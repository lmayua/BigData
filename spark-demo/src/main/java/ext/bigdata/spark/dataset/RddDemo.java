package ext.bigdata.spark.dataset;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class RddDemo {
    public static void main(String[] args) throws IOException {
        // Method1
        //SparkConf sc = new SparkConf().setAppName("${appName}").setMaster("${master}").set("${key}", "${value}").setJars(new String[] { "${jar1Name}", "${jar2Name}", "..."});
        //JavaSparkContext jsc = new JavaSparkContext(sc);
        SparkConf sc = new SparkConf().setAppName("appName").setMaster("local");// master: local->本地运行;spark://master:port->单节点运行
        JavaSparkContext jsc = new JavaSparkContext(sc);

        // Method2
        //JavaSparkContext jsc = new JavaSparkContext("${master}", "$appName", "$sparkHome", new String[] { "${jar1Name}", "${jar2Name}", "..."}, Map<String, String> env);
        //JavaSparkContext jsc = new JavaSparkContext("local", "appName");

        // 读取HBase
        //Configuration hbaseConf = getHBaseConfiuration();// 获取HBase配置信息，用于生成RDD
        
        /*SQLContext sqlContext = new HiveContext(jsc); // 老版本读取HIVE方式，2.0以上版本同意使用SparkSession入口
        sqlContext.sql("${hive sql}");*/

        // source: 
        JavaRDD<String> rdd = jsc
                //.textFile("${textFilePath}", ${int_partitionNum});//
                //.hadoopFile("${hdfsPath}", ${class_inputFormateClass}, ${class_outKeyClass}, ${class_outValueClass}, ${int_minPartitions}) -> JavaPairRDD
                //.newAPIHadoopFile(path, fClass, kClass, vClass, conf)
                //.newAPIHadoopRDD(hbaseConf, fClass, kClass, vClass);// hadoop的数据源包括：HBase
                //.sequenceFile(path, keyClass, valueClass)// 读取sequence文件
                .textFile("E:/Work/tmp/test_data/da-bi_da_oms_detail-consumer.txt");

        //rdd.cache();// 1.后边不能接算子(非action);2.cache是persist的特殊状态，使其失效调用.unpersist();
        //rdd.persist(StorageLevel.MEMORY_AND_DISK());
        //rdd.checkpoint();// 在对RDD做checkpoint操作之前必须做persist，checkpoint是懒加载，执行完job后会重头计算该RDD

        //算子transfermation -> action
        //.map(Function<${inputType}, ${outputType}>)
        //.mapToPair(PairFunction<String, K2, V2>)
        //.mapPartitions(FlatMapFunction<Iterator<${inputType}>, ${outputType}>);
        //.flatMap(FlatMapFunction<${inputType}, ${outputType}>)
        //.filter(Function<${inputType}, Boolean>)
        //.union(${JavaRDD})
        //.distinct(${O_int_numPartition})
        //.cartesian(${JavaRDD})// 笛卡尔积

        //rdd.sample(false, 0.5, System.currentTimeMillis());// 抽样函数;抽样是否放回,抽样率,随机数种子

        System.out.println(rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String msg) throws Exception {

                return Arrays.asList(new String[]{msg}).iterator();
            }
        }).collect());

    }
}
