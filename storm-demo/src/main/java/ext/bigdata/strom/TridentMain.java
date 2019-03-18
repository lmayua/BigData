/*
 * Copyright (C), 2002-2018, 苏宁易购电子商务有限公司
 * FileName: TridentMain.java
 * Author:   17073580
 * Date:     2018年2月9日 上午10:08:51
 * Description: <模块目的、功能描述>
 * History: <修改记录>
 * <author>   <time>   <version>  <desc>
 * 修改人姓名        修改时间       版本号                  描述
 */
package ext.bigdata.strom;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

public class TridentMain {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        Config conf = new Config();
        
        TridentTopology tridentTopology = new TridentTopology(); // 创建Trident拓扑
        
        tridentTopology.newStream("word-reader-stream", new WordReader())
                .each(new Fields("line"), new NormalizeFunction(), new Fields("word"))
                .filter(new demoFilter())
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Sum(), new Fields("sum"))
                .parallelismHint(16);
        
        StormTopology stormTopology = tridentTopology.build(); // 将Trident拓扑转换成Storm拓扑
        
        // 集群模式
        StormSubmitter.submitTopology("demoTopology", conf, stormTopology);
    }
}
class demoFilter implements Filter{

    @Override
    public void cleanup() {
    }

    @Override
    public void prepare(Map arg0, TridentOperationContext arg1) {
    }

    @Override
    public boolean isKeep(TridentTuple arg0) {
        return false;
    }
}


class WordReader implements ITridentSpout<String> {

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator<String> getCoordinator(String arg0, Map arg1,
            TopologyContext arg2) {
        return null;
    }

    @Override
    public org.apache.storm.trident.spout.ITridentSpout.Emitter<String> getEmitter(String arg0, Map arg1, TopologyContext arg2) {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }

}

class NormalizeFunction implements Function {

    @Override
    public void cleanup() {
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentoperationcontext) {
    }

    @Override
    public void execute(TridentTuple tridenttuple, TridentCollector tridentcollector) {
    }

}

class Sum implements CombinerAggregator<String> {

    @Override
    public String combine(String arg0, String arg1) {
        return null;
    }

    @Override
    public String init(TridentTuple tridenttuple) {
        return null;
    }

    @Override
    public String zero() {
        return null;
    }

}
