package bdconsistency;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import bdconsistency.query.PrinterBolt;
import bdconsistency.trade.Trade;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.*;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/11/13
 * Time: 1:47 PM
 */
public class Counter {
    public static class CountAggregator implements Aggregator<Double> {
        long count;
        @Override
        public Double init(Object batchId, TridentCollector collector) {
            return 0D;
        }

        @Override
        public void aggregate(Double val, TridentTuple tuple, TridentCollector collector) {
            if (tuple.getString(0).startsWith("BIDS")) {
                //Trade t = new Trade(tuple.getString(0).split("\\|"));
                // System.out.println(MessageFormat.format("Reducing...{0}{1}", t.getOrderId(), t.getOrderId()));
                count += 1;
                val = 1.0 * count;
            /*
                if (t.getOperation() == 1) val += t.getVolume();
                else val -= t.getVolume();
            */
                if (val % 1000 == 0)
                    collector.emit(new Values(count));
            }
        }

        @Override
        public void complete(Double val, TridentCollector collector) {
            collector.emit(new Values(val));
        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {}

        @Override
        public void cleanup() {}
    }

    public static class Split extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            for(String word: tuple.getString(0).split("\\|")) {
                if(word.length() > 0) {
                    collector.emit(new Values(word));
                }
            }
        }
    }

    public static StormTopology buildTopology(String fileName) {
        TridentTopology topology = new TridentTopology();
        final ITridentSpout asksSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));
        TridentState state = topology.newStaticState(new MemoryMapState.Factory());

        Stream volumes = topology
                .newStream("spout", asksSpout);

        volumes.aggregate(new Fields("tradeString"), new CountAggregator(), new Fields("count"))
                .project(new Fields("count"))
                .each(new Fields("count"), new PrinterBolt());

        return topology.build();
    }

    public static void main(String[] args) {
        Config conf = new Config();
        //conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("VolumeCounterTopology", conf, buildTopology(args[0]));
        try {
            Thread.sleep(300000);
        } catch (InterruptedException ignore) {}
        cluster.killTopology("VolumeCounterTopology");
        cluster.shutdown();
    }
}
