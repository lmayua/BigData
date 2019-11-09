package ext.bigdata.hbase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase工具类
 */
public class HBaseUtil {

    private static final String HBASE_ZK_QUORUM = "";

    private static final String HBASE_ZK_PROP_PORT = "2015";
    
    private static final String CHARSET_UTF8 = "utf-8";
    
    private static final String HBASE_PARENT_PATH = "/hbase";
    
    private static final Log LOG = LogFactory.getLog(HBaseUtil.class);

    private static Configuration conf;

    private static HConnection connection;

    private static HTable htable = null;

    private HBaseUtil() {
        super();
    }

    /**
     * 获取HBase连接
     */
    private static HConnection getConnection() throws IOException {
        if (connection == null) {
            synchronized (HBaseUtil.class) {
                if (null == conf) {
                    System.setProperty("HADOOP_USER_NAME", "erp");
                    System.setProperty("HADOOP_GROUP_NAME", "erp");

                    conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
                    conf.set("hbase.zookeeper.property.clientPort", HBASE_ZK_PROP_PORT);
                    conf.set("zookeeper.znode.parent", HBASE_PARENT_PATH);

                    //conf.set("hbase.client.retries.number", "");
                    //conf.set("hbase.client.pause", "");
                    //conf.set("zookeeper.recovery.retry.intervalmill", "");
                    //conf.set("ipc.socket.timeout", "");
                    //conf.set("hbase.rpc.timeout", "");
                    //conf.set("hbase.client.scanner.timeout.period", "");
                }

                if (connection == null) {
                    connection = HConnectionManager.createConnection(conf);
                }
            }
        }
        return connection;
    }

    /**
     * 单列添加
     *
     * @param tableName
     * @param columnfamily
     * @param qualifier
     * @param rowKey
     * @param value
     * @return
     */
    public static boolean put(String tableName, String columnfamily, String qualifier, String rowKey, String value) {
        HTable table = null;
        try {
            table = (HTable) getConnection().getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(columnfamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.flushCommits();
        } catch (IOException e) {
            LOG.error("HBASE添加数据出现异常:" + e.getMessage(), e);
            return false;
        } catch (Exception e) {
            LOG.error("HBASE添加数据出现异常:" + e.getMessage(), e);
            return false;
        }
        return true;
    }

    /**
     * 批量插入
     *
     * @param tableName 表明
     * @param putList PUT集合
     * @return 是否成功插入
     */
    public static boolean batchPuts(String tableName, List<Put> putList) {
        HTable htable = null;
        try {
            htable = (HTable) getConnection().getTable(tableName);
            htable.put(putList);
            htable.flushCommits();
        } catch (IOException e) {
            LOG.error("HBASE链接出现异常:" + e.getMessage(), e);
            return false;
        } catch (Exception e) {
            LOG.error("插入HBASE出现异常:" + e.getMessage(), e);
            return false;
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    LOG.error("关闭table异常:" + e.getMessage(), e);
                }
            }
        }
        return true;
    }

    /**
     * 批量查询
     *
     * @param tableName
     * @param getList
     * @return
     */
    public static Result[] batchGets(String tableName, List<Get> getList) {
        Result[] results = null;
        try {
            htable = (HTable) getConnection().getTable(tableName);
            results = htable.get(getList);
        } catch (IOException e) {
            LOG.error("HBASE批量查询出现异常:" + e.getMessage(), e);
        }

        return results;
    }

    /**
     * 批量查询
     *
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param colArr
     * @return
     */
    public static Result[] batchGets(String tableName, String rowkey, String columnFamily, String[] colArr) {

        List<Get> getList = new ArrayList<>();
        for (String col : colArr) {
            Get get;
            get = new Get(rowkey.getBytes(Charset.forName(CHARSET_UTF8)));
            get.addColumn(columnFamily.getBytes(Charset.forName(CHARSET_UTF8)), col.getBytes(Charset.forName(CHARSET_UTF8)));
            getList.add(get);
        }

        return batchGets(tableName, getList);
    }

    /**
     * 批量删除
     *
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param colArr
     * @return
     */
    public static boolean batchDeletes(String tableName, String rowkey, String columnFamily, String[] colArr) {
        List<Delete> deleteList = new ArrayList<>();

        for (String col : colArr) {
            Delete del = new Delete(rowkey.getBytes(Charset.forName(CHARSET_UTF8)));
            del.deleteColumn(columnFamily.getBytes(Charset.forName(CHARSET_UTF8)), col.getBytes(Charset.forName(CHARSET_UTF8)));
            deleteList.add(del);
        }

        return batchDeletes(tableName, deleteList);
    }

    /**
     * 批量删除
     *
     * @param tableName
     * @param deleteList
     * @return
     */
    public static boolean batchDeletes(String tableName, List<Delete> deleteList) {
        try {
            htable = (HTable) getConnection().getTable(tableName);
            htable.delete(deleteList);
        } catch (IOException e) {
            LOG.error("HBASE批量删除出现异常:" + e.getMessage(), e);
            return false;
        }
        return true;
    }
}
