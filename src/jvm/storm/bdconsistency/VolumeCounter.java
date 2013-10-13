package bdconsistency;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
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
import com.google.common.collect.Lists;
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
    public static class VolumeAggregator implements Aggregator<VolumeAggregator.CountState> {
        public class CountState{
            double volume;
            double count;
        }

        @Override
        public VolumeAggregator.CountState init(Object batchId, TridentCollector collector) {
            return new CountState();
        }

        @Override
        public void aggregate(CountState val, TridentTuple tuple, TridentCollector collector) {
            Trade t = new Trade(tuple.getString(0).split("\\|"));
            val.count++;

            if (t.getOperation() == 1) {
                val.volume += t.getVolume();
            } else{
                val.volume -= t.getVolume();
            }

            collector.emit(new Values(val.volume));
            collector.emit(new Values(val.count));
        }

        @Override
        public void complete(CountState val, TridentCollector collector) {

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

        Stream volumes = topology
                .newStream("spout", asksSpout)
                .parallelismHint(8);

        Stream aggregates = volumes.shuffle()
                .aggregate(new Fields("tradeString"), new VolumeAggregator(), new Fields("volume", "count"))
                .project(new Fields("volume", "count"))
                .parallelismHint(8)
                ;

        aggregates.each(new Fields("volume","count"), new PrinterBolt());

        return topology.build();
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        Config conf = new Config();
        conf.setNumWorkers(20);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("damsel", "qp4", "qp5", "qp6"));
        conf.setMaxSpoutPending(4);
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        //StormSubmitter.submitTopology("VolumeCounter", conf, buildTopology(args[0]));

        // Let it run for 5 minutes
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("VolumeCounterTopology", conf, buildTopology(args[0]));
        Thread.sleep(300000);
        cluster.killTopology("VolumeCounterTopology");
        cluster.shutdown();
    }
}
