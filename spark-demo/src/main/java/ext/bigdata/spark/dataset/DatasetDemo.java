package ext.bigdata.spark.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Encode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DatasetDemo {
    public static void main(String[] args) {

        // Dataset是spark2.0版本引入的

        SparkSession session = SparkSession.builder().master("local").appName("local-app").getOrCreate();


       /**
        * data source
        *
        * 1.hive
        * 2.csv file
        * 3.text file
        * 4.json
        * 5.jdbc
        * 6.javaRDD
        */

        //Dataset<Row> ds = session.sql("${sql}");
        //Dataset<Row> ds = session.read().option("header","true").csv("${path}");
        //Dataset<Row> ds = session.read().json("${json}");
        //Dataset<Row> ds = session.read().jdbc(url, table, properties);

        // JSON String list -> Dataset
        //JavaRDD<String> confRdd = new JavaSparkContext(session.sparkContext()).parallelize(${ArrayList<String>()});
        //Dataset<Row> ds = session.read().json(confRdd);

        Dataset<Row> ds = session.read().text("E:\\Work\\tmp\\test_data\\da-bi_da_oms_detail-consumer.txt");


       /**
        * dataset function
        *
        * 1.transform
        * filter()
        * sample()
        * map()
        * mapPartitions()
        * flatMap()
        * groupByKey()
        * repartition()
        * join()
        * distinct()
        *
        * union()
        * sort(),sortWithPartitions()
        *
        *
        * 2.action
        * reduce()
        * collect()
        * count()
        * show()
        * first()
        * take()
        * takeSample()
        * saveAsTextFile()
        * saveAsSequenceFile()
        * saveAsObjectFile()
        * forEach()
        * countByKey()
        */

        /*ds.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row row) throws Exception {
                // to filter row
                return false;
            }
        });*/

        // 是否抽样后放回;抽样率;抽样算法的随机数种子,默认随机数,可选
        //ds.sample(false, 0.5, System.currentTimeMillis());

        /*ds.flatMap(new FlatMapFunction<Row, Object>() {

            @Override
            public Iterator<Object> call(Row row) throws Exception {
                return null;
            }
        }, Encoders.javaSerialization(${javaObject.class}));*/

        ds.map(new MapFunction<Row, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row row) throws Exception {
                System.out.println(row.get(0));
                return row.getString(0);
            }
        }, Encoders.STRING());


        // 分组计算
        /*KeyValueGroupedDataset<Object, Row> kvGroupDs = ds.groupByKey(new MapFunction<Row, Object>() {

            @Override
            public Object call(Row row) throws Exception {
                return null;
            }
        }, Encoders.javaSerialization(${javaObject.class}));

        kvGroupDs.flatMapGroups(new FlatMapGroupsFunction<Object, Row, Object>() {
            @Override
            public Iterator<Object> call(Object o, Iterator<Row> iterator) throws Exception {
                return null;
            }
        }, Encoders.javaSerialization(${javaObject.class}));*/

        //ds.union(${otherDataset});
        //ds.sort(...);

    }
}
