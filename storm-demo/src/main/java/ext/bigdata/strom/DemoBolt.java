package ext.bigdata.strom;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * <Bolt组件Demo> 
 */
public class DemoBolt implements IRichBolt{

    @Override
    public void execute(Tuple input) {
        String msg = input.getString(0);
        
        // deal message
        doSometing();
        
        // emit message
        collector.emit(new Values(msg));
        
        // mark tuple with done status
        collector.ack(input);
    }
    
    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    
    /**
     */
    private OutputCollector collector;
    
    /**
     * serial
     */
    private static final long serialVersionUID = -2591131165162823037L;

    /*public void execute(Tuple input) {
        String msg = input.getString(0);
        
        // deal message
        System.out.println("Demo bolt message:" + msg);
        
        // emit message
        collector.emit(new Values(msg));
        
        // mark tuple with done status
        collector.ack(input);
    }*/

    private void doSometing() {}
    /* 
     * work before shutdown
     * @see org.apache.storm.task.IBolt#cleanup()
     */
    public void cleanup() {
        
        System.out.println("Demo bolt clean up");
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
