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
        conf.setMaxSpoutPending(2);
        conf.put("topology.spout.max.batch.size", 1000);
        conf.put("topology.trident.batch.emit.interval.millis", 500 );
        //conf.put(Config.DRPC_SERVERS, Lists.newArrayList("damsel", "qp4", "qp5", "qp6"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        return conf;
    }

    public static Config getBaseConfig() throws IOException {
        Properties prop = new Properties();
        Config conf = new Config();
        //load a properties file
        //prop.load(new FileInputStream("storm.basic.properties"));
        prop.load(PropertiesReader.class.getClassLoader().getResourceAsStream("storm.basic.properties"));
        return conf;
    }
}
