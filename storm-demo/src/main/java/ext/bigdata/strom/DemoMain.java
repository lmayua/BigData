package ext.bigdata.strom;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class DemoMain {
    public static void main(String[] args) throws Exception {
        
        TopologyBuilder topoBuilder = new TopologyBuilder();// 创建TopologyBuilder
        topoBuilder.setSpout("demoSpout", new DemoSpout()).setNumTasks(4);// 设置Spout，Task并行数为4
        topoBuilder.setBolt("demoBolt1", new DemoBolt()).setNumTasks(4).shuffleGrouping("demoSpout");// 设置Bolt1
        topoBuilder.setBolt("demoBolt2", new DemoBolt(), 1).shuffleGrouping("demoBolt1");// 设置Bolt2
        
        Config conf = new Config();
        conf.setNumWorkers(4);// 设置Work并行数为4
        conf.put("variable-1", "E:\\Work\\tmp\\test.txt");
        conf.setDebug(true);
        
        // 本地模式
        /*LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("demoTopology", conf, topoBuilder.createTopology());*/
        
        // 集群模式
        StormSubmitter.submitTopology("demoTopology", conf, topoBuilder.createTopology());
        
        /*Thread.sleep(20000);
        System.out.println("local cluster shutdown...");
        localCluster.shutdown();*/
        
        /*SpoutConfig spoutConfig = new SpoutConfig(null, "", "", "");
        FixedBatchSpout fixedBatchSpout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        new Split();
        new Sum();
        new MemoryMapState.Factory();
        TridentTopology topo = new TridentTopology();*/
        //spoutConfig.bufferSizeBytes
        //spoutConfig.clientId
        //spoutConfig.failedMsgRetryManagerClass
        //spoutConfig.fetchMaxWait
        //spoutConfig.fetchSizeBytes
        //spoutConfig.hosts
        //spoutConfig.id
        //spoutConfig.ignoreZkOffsets
        //spoutConfig.maxOffsetBehind
        //spoutConfig.metricsTimeBucketSizeInSecs
        //spoutConfig.minFetchByte;
        //spoutConfig.outputStreamId
        //spoutConfig.retryDelayMaxMs
        //spoutConfig.retryDelayMultiplier
        //spoutConfig.retryInitialDelayMs
        //spoutConfig.retryLimit
        //spoutConfig.scheme
        //spoutConfig.socketTimeoutMs
        //spoutConfig.startOffsetTime
        //spoutConfig.startOffsetTime
        //spoutConfig.stateUpdateIntervalMs
        //spoutConfig.topic //kafka topic name
        //spoutConfig.useStartOffsetTimeIfOffsetOutOfRange
        //spoutConfig.zkPort
        //spoutConfig.zkRoot
        //spoutConfig.zkServers
    }
}
