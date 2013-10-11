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
import storm.trident.operation.*;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/11/13
 * Time: 1:47 PM
 */
public class VolumeCounter {
    public static class VolumeAggregator implements Aggregator<Double> {
        public Double init() {
            return 0D;
        }

        public Double reduce(Double state, TridentTuple tuple) {
            System.out.println("Reducing...");
            Trade t = new Trade(tuple.getString(0).split("\\|"));
            if (t.getOperation() == 1) state += t.getVolume();
            else state -= t.getVolume();
            System.out.println("returning state value -- " + state);
            return state;
        }

        @Override
        public Double init(Object batchId, TridentCollector collector) {
            return 0D;
        }

        @Override
        public void aggregate(Double val, TridentTuple tuple, TridentCollector collector) {
            Trade t = new Trade(tuple.getString(0).split("\\|"));
            if (t.getOperation() == 1) val += t.getVolume();
            else val -= t.getVolume();
            collector.emit(new Values(val));
        }

        @Override
        public void complete(Double val, TridentCollector collector) {
            //To change body of implemented methods use File | Settings | File Templates.
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
/*                .each(new Fields("tradeString"), new Split(), new Fields("table", "op", "ts", "broker", "price", "volume"))
                .stateQuery(state, new Fields("op", "volume"), new);*/

        volumes.aggregate(new Fields("tradeString"), new VolumeAggregator(), new Fields("volume"))
        .each(new Fields("volume"), new PrinterBolt());

        return topology.build();
    }

    public static void main(String[] args) {
        Config conf = new Config();
        //conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("VolumeCounterTopology", conf, buildTopology(args[0]));
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ignore) {}
        cluster.killTopology("VolumeCounterTopology");
        cluster.shutdown();
    }
}
