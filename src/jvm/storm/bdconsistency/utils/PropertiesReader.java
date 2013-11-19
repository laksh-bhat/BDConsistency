package bdconsistency.utils;

import backtype.storm.Config;
import com.google.common.collect.Lists;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * User: lbhat@damsl
 * Date: 10/30/13
 * Time: 6:13 PM
 */
public class PropertiesReader {
    public static Config getStormConfig () {
        Config conf = new Config();
        conf.setNumAckers(16);
        conf.setNumWorkers(16);
        conf.setMaxSpoutPending(16);
        conf.put("topology.spout.max.batch.size", 1000);
        conf.put("topology.trident.batch.emit.interval.millis", 500);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd1", "qp-hd-6", "qp-hd5", "qp-hd7", "qp-hd8", "qp-hd9", "qp-hd3", "qp-hd4"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        //conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 300);
        return conf;
    }

    public static Config getBaseConfig () throws IOException {
        Properties prop = new Properties();
        Config conf = new Config();
        prop.load(PropertiesReader.class.getClassLoader().getResourceAsStream("storm.basic.properties"));
        return conf;
    }
}
