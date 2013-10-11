package bdconsistency;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import bdconsistency.ask.AsksStateFactory;
import bdconsistency.ask.AsksUpdater;
import bdconsistency.bid.BidsStateFactory;
import bdconsistency.bid.BidsUpdater;
import bdconsistency.query.AsksBidsJoin;
import bdconsistency.query.AxFinderFilter;
import bdconsistency.query.BrokerEqualityQuery;
import bdconsistency.query.PrinterBolt;
import bdconsistency.trade.Trade;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

/**
 * User: lbhat@damsl
 * Date: 10/11/13
 * Time: 1:47 PM
 */
public class VolumeCounter {
    public static class VolumeAggregator implements ReducerAggregator<Long> {
        @Override
        public Long init() {
            return 0L;
        }

        @Override
        public Long reduce(Long state, TridentTuple tuple) {
            System.out.println("Reducing...");
            Trade t = new Trade(tuple.getString(0).split("\\|"));
            if (t.getOperation() == 1) state += t.getVolume();
            else state -= t.getVolume();
            System.out.println("returning state value -- " + state);
            return state;
        }
    }

    public static StormTopology buildTopology(String fileName) {
        TridentTopology topology = new TridentTopology();
        final ITridentSpout asksSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));

        Stream asks = topology
                .newStream("spout1", asksSpout)
                .aggregate(new Fields("tradeString"), new VolumeAggregator(), new Fields("volume"))
                .each(new Fields("volume"), new PrinterBolt());
        return topology.build();
    }

    public static void main(String[] args) {
        Config conf = new Config();
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("VolumeCounterTopology", conf, buildTopology(args[0]));
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ignore) {}
        cluster.killTopology("VolumeCounterTopology");
        cluster.shutdown();
    }
}
