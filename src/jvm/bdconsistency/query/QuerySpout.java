package jvm.bdconsistency.query;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/7/13
 * Time: 8:51 AM
 */
public class QuerySpout implements IRichSpout {
    public static Logger LOG = Logger.getLogger(QuerySpout.class);
    SpoutOutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 10;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void activate() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void deactivate() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void nextTuple() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void ack(Object msgId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void fail(Object msgId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
