package bdconsistency;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import bdconsistency.query.AxFinderFilter;
import bdconsistency.query.PrinterBolt;
import bdconsistency.trade.Trade;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/13/13
 * Time: 9:45 PM
 */
public class Filter {

    public static class CountAggregator implements Aggregator<CountAggregator.State> {
        static class State{
            long count;
            long volume;
        }
        @Override
        public CountAggregator.State init(Object batchId, TridentCollector collector) {
            return new State();
        }

        @Override
        public void aggregate(CountAggregator.State val, TridentTuple tuple, TridentCollector collector) {
            val.count += 1;
            val.volume = Long.valueOf(tuple.getString(0).split("\\|")[6]);
        }

        @Override
        public void complete(State val, TridentCollector collector) {
            collector.emit(new Values(val.count, val.volume));
        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {}

        @Override
        public void cleanup() {}
    }

    public static StormTopology buildTopology(String fileName) {
        TridentTopology topology = new TridentTopology();
        final ITridentSpout asksSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));
        final ITridentSpout bidsSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));

        Stream asksStream = topology.newStream("asks", asksSpout);
        Stream bidsStream = topology.newStream("bids", bidsSpout);

        asksStream
                .each(new Fields("tradeString"), new AxFinderFilter.AsksFilter())
                .aggregate(new Fields("tradeString"), new CountAggregator(), new Fields("count", "volume"))
                .project(new Fields("count"))
                .each(new Fields("count"), new PrinterBolt.AsksPrinterBolt());

        bidsStream
                .each(new Fields("tradeString"), new AxFinderFilter.BidsFilter())
                .aggregate(new Fields("tradeString"), new CountAggregator(), new Fields("count","volume"))
                .project(new Fields("count", "volume"))
                .each(new Fields("count", "volume"), new PrinterBolt.BidsPrinterBolt());

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
