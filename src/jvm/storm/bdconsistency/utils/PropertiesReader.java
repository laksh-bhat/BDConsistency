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
    public static Config getStormConfig() {
        Config conf = new Config();
        conf.setNumAckers(8);
        conf.setNumWorkers(8);
        conf.setMaxSpoutPending(4);
        conf.put("topology.spout.max.batch.size", 100);
        conf.put("topology.trident.batch.emit.interval.millis", 100 );
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("qp-hd1", "qp-hd9", "qp-hd3", "qp-hd4"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        return conf;
    }

    public static Config getBaseConfig() throws IOException {
        Properties prop = new Properties();
        Config conf = new Config();
        prop.load(PropertiesReader.class.getClassLoader().getResourceAsStream("storm.basic.properties"));
        return conf;
    }
}
