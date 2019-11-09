package ext.bigdata.spark.limitstream;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.RateLimiter;

public class LimitDemo {
    public static void main(String[] args) {
        
        LimitDemo demo = new LimitDemo();
        List<Object> boList = demo.convertDatas();
        //System.setProperty("hadoop.home.dir", "E:\\Tools\\spark-2.2.0-bin-hadoop2.7");
        //System.setProperty("user.name", "hive");
        //System.setProperty("HADOOP_USER_NAME", "erp");
        SparkSession session = SparkSession.builder()
                                               .master("local")
                                               .appName("local-app")
                                               //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                                               .enableHiveSupport()
                                               .getOrCreate();

        int limit = 1000; // 限流
        //int executorNum = Integer.parseInt(session.sparkContext().conf().get("spark.executor.instances"));
        int executorNum = 2; // 执行器数
        int coreNum = 3 * executorNum; // 核数
        int parallelism = coreNum * 2; // 并行数
        int perPartitionLimit = limit / parallelism; // 每个分区的限流大小 = 总限流大小 / 分区数(并行数)

        System.out.println(MessageFormat.format("coreNum: {0}, parllelism: {1}, perPartitionLimit: {2}", coreNum,
                String.valueOf(parallelism), String.valueOf(perPartitionLimit)));
        
        session.sparkContext().conf().set("spark.defalut.parallelism", String.valueOf(parallelism)); // 设置并行度
        //Dataset<Row> ds = session.read().text("E:/Work/tmp/test_data/1.txt"); // source
        //session.sql("CREATE TABLE IF NOT EXISTS test.ftp_crt_tab_1 ( `id` STRING COMMENT 'id', `name` STRING COMMENT 'name', ETL_TIME STRING COMMENT 'ETL处理时间' ) STORED AS RCFILE");
        Dataset<Row> ds = session.sql("select * from test.emp");
        
        Dataset<String> mapDs = ds.mapPartitions(new MapPartitionsFunction<Row, String>() {

            /**
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Iterator<Row> iterator) throws Exception {
                RateLimiter limiter = RateLimiter.create(perPartitionLimit);
                //ServiceAgent serviceAgent = ServiceLocator.getServiceAgent("com.suning.rsf.cmcdc.service.PointQueryService",null, false);
                List<String> resultList = new ArrayList<>();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    Map<String, Object> rowMap = new HashMap<>();
                    rowMap.put("empno", row.getString(0)); // empno
                    rowMap.put("empname", row.getString(1));
                    rowMap.put("sal", row.getString(4));
                    
                    System.out.println(row);
                    limiter.acquire();
                    
                    resultList.add(new JSONObject(rowMap).toJSONString());
                    //System.out.println(serviceAgent.invoke("queryContractPoint", new Object[] { boList }, new Class[] { List.class }));
                }
                
                //Row row = new GenericRow(new Object[]{"a", "b", "c"});
                //String jsonStr = JSONObject.parse("{'name1':'a', 'name2':'b','name3':'c'}").toString();
                //System.out.println(jsonStr);
                //resultList.add(jsonStr);
                
                return resultList.iterator();
            }
            
        }, Encoders.STRING());
        StructType st = new StructType();
        st = st.add("empno", DataTypes.StringType, false).add("empname", DataTypes.StringType, false).add("sal", DataTypes.StringType, false);
        
        //session.createDataFrame(mapDs.rdd(), st).show();
        Dataset<Row> rstDs = session.read().schema(st).json(mapDs.toJavaRDD());
        //Dataset<Row> rstDs = session.read().json(mapDs.toJavaRDD());
        rstDs.show();
        System.out.println(rstDs.schema());
        rstDs.printSchema();
        rstDs.createOrReplaceTempView("emp_tmp");
        // ds.write().partitionBy("${partitionField}").mode(SaveMode.Overwrite).saveAsTable("${tableName}");
        session.sql("insert overwrite table test.emp_last select * from emp_tmp");
    }

    // TODO
    private List<Object> convertDatas() {
        return null;
    }
}
