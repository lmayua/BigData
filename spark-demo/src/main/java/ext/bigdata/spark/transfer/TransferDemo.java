package ext.bigdata.spark.transfer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TransferDemo {
    public static void main(String[] args) {
        //@formatter:off
        SparkSession session = SparkSession.builder()
                                               .master("local")
                                               .appName("transfer-demo")
                                               .enableHiveSupport()
                                               .getOrCreate();
        
        Dataset<Row> ds = session.read()
                                    .format("com.springml.spark.sftp")
                                    .option("host", "10.27.206.154")
                                    .option("username", "root")
                                    .option("password", "dWat_9Z4")
                                    .option("fileType", "txt")
                                    .load("/opt/wildfly/algorithms/log/socket_serv");
                                    ;
        
        ds.mapPartitions(new MapPartitionsFunction<Row, String>() {
            @Override
            public Iterator<String> call(Iterator<Row> input) throws Exception {
                
                List<String> resultList = new ArrayList<>();
                while (input.hasNext()){
                    String msg = String.valueOf(input.next().get(0));
                    System.out.println(msg);
                    resultList.add(msg);
                }
                
                return resultList.iterator();
            }
        }, Encoders.STRING()).show();
        //@formatter:on
    }
}
