package ext.bigdata.strom;

import java.util.List;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * <Spout组件Demo>
 */
public class DemoSpout implements IRichSpout {

    @Override
    public void ack(Object msgId) {
        // 在检测到一个tuple被整个topology成功处理的时候调用ack
    }
    
    public void fail(Object msgId) {
        // 在检测到一个tuple被整个topology处理失败时，调用fail
    }
    
    @Override
    public void nextTuple() {
        // 1.直接返回
        if (someCondition()){
            return;
        }
        // 2.向Topology发射新Tuple
        List<Object> tupel = new Values("some message");
        this.collector.emit(tupel);
    }
    
    /**
     * serial ID
     */
    private static final long serialVersionUID = 3304109231564150661L;

    /**
     * collector
     */
    private SpoutOutputCollector collector;

    /**
     * 拓扑上下文
     */
    private TopologyContext context;

    /**
     * 配置信息
     */
    private Map<String, Object> conf;
    
    private boolean isComplete = false;

    public void activate() {}

    /* 
     * shutdown
     * @see org.apache.storm.spout.ISpout#close()
     */
    public void close() {}

    public void deactivate() {}

    /* 
     * @see org.apache.storm.spout.ISpout#nextTuple()
     */
    /*public void nextTuple() {
        
        if (this.isComplete)
        {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
            }
            return;
        }
        
        Object argObj = this.conf.get("variable-1");
        String arg = null == argObj ? "" : String.valueOf(argObj);
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new FileReader(new File(arg)));

            String line;
            while (null != (line = bf.readLine())) {
                this.collector.emit(new Values(line));// emit message
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("error msg", e);
        } catch (IOException e) {
            throw new RuntimeException("error msg", e);
        } finally {
            if (null != bf) {
                try {
                    bf.close();
                } catch (IOException e) {
                }
            }
            this.isComplete = true;
        }

    }*/
    
    private boolean someCondition() {
        // TODO Auto-generated method stub
        return false;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.conf = conf;
    }

    /* 
     * declare field
     * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("spout-fields"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
