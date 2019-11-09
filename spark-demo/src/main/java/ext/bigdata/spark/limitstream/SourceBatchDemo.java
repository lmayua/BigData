package ext.bigdata.spark.limitstream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SourceBatchDemo {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local").appName("local-app").getOrCreate();
        String countHql = "select count(*) from ${tableName}"; // 获取记录数count
        int count = 10000;
        int recordNum = 1000;
        int pageNum = count / recordNum;
        
        for (int i = 1;i <= pageNum;i++){
            int startNum = recordNum * i + 1;
            int endNum = recordNum * (i + 1) - 1; 
            
            String hql = "select * from (select row_number() over (order by ${field} desc) as rnum ,t1.* from ${tableName}t1 )t where rnum >= ${startNum} and rnum <= ${endNum}";
            Dataset<Row> rows = session.sql(hql);
            // map -> reduce -> interface
            //  
        }
        // last page
    }
}
