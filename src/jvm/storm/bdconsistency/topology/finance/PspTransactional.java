package bdconsistency.topology.finance;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DRPCClient;
import bdconsistency.state.ask.AsksStateFactory;
import bdconsistency.state.ask.AsksUpdater;
import bdconsistency.state.bid.BidsStateFactory;
import bdconsistency.state.bid.BidsUpdater;
import bdconsistency.state.counter.CounterState;
import bdconsistency.bolt.trident.filter.AxFinderFilter;
import bdconsistency.bolt.trident.filter.PrinterBolt;
import bdconsistency.bolt.trident.query.PspQuery;
import bdconsistency.bolt.trident.query.SelectQuery;
import bdconsistency.spouts.TransactionalTextFileSpout;
import bdconsistency.utils.PropertiesReader;
import org.apache.thrift7.TException;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.ITridentSpout;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static bdconsistency.topology.TopologyBase.cleanup;
import static bdconsistency.topology.TopologyBase.printTimings;

public class PspTransactional {
    public static class CountUpdater implements StateUpdater<CounterState> {
        @Override
        public void prepare(Map conf, TridentOperationContext context) {}
        @Override
        public void cleanup() {}
        @Override
        public void updateState(CounterState state, List<TridentTuple> tuples, TridentCollector collector) {
            for (int i = 0; i < tuples.size(); i++)
                state.increment();
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc, String fileName, long stateSize) {
        final ITridentSpout asksSpout  = new TransactionalTextFileSpout("tradeString", fileName, "UTF-8");
        final ITridentSpout bidsSpout  = new TransactionalTextFileSpout("tradeString", fileName, "UTF-8");
        final TridentTopology topology = new TridentTopology();

        final Stream asksStream = topology
                .newStream("spout1", asksSpout).each(new Fields("tradeString"), new AxFinderFilter.AsksFilter());
        final Stream bidsStream = topology
                .newStream("spout2", bidsSpout).each(new Fields("tradeString"), new AxFinderFilter.BidsFilter());

        // In this state we will save the asks table
        TridentState asks = asksStream
                .partitionBy(new Fields("tradeString"))
                .partitionPersist(new AsksStateFactory(stateSize), new Fields("tradeString"), new AsksUpdater())
                .parallelismHint(8)
        ;

        // In this state we will save the bids table
        TridentState bids = bidsStream
                .partitionBy(new Fields("tradeString"))
                .partitionPersist(new BidsStateFactory(stateSize), new Fields("tradeString"), new BidsUpdater())
                .parallelismHint(8)
        ;

        TridentState count =
                topology.merge(asksStream, bidsStream)
                .shuffle()
                .partitionPersist(new CounterState.CounterStateFactory(), new Fields("tradeString"), new CountUpdater())
        ;

        // DRPC Service
        topology
                .newDRPCStream("PSP", drpc)
                .each(new Fields("args"), new PrinterBolt())
                .stateQuery(asks, new SelectQuery.SelectStarFromAsks(), new Fields("asks"))
                .parallelismHint(8)
                .shuffle()
                .stateQuery(bids, new SelectQuery.SelectStarFromBids(), new Fields("bids"))
                .parallelismHint(8)
                .shuffle()
                .each(new Fields("asks", "bids"), new PspQuery(), new Fields("PSP"))
                .project(new Fields("PSP"))
                .shuffle()
                .stateQuery(count, new BaseQueryFunction<CounterState, Object>() {
                    @Override
                    public List<Object> batchRetrieve(CounterState state, List<TridentTuple> args) {
                        List<Object> batch = new ArrayList<Object>();
                        for (int i = 0; i < args.size(); i++)
                            batch.add(state.getCount());
                        return batch;
                    }
                    @Override
                    public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
                        collector.emit(new Values(result));
                    }
                }, new Fields("count"))
                .parallelismHint(8);

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        FinanceTopology.checkArguments(args);
        Config config = PropertiesReader.getStormConfig();
        SubmitTopologyAndRunDrpcQueries(args, "PSP", config);
    }

    public static void SubmitTopologyAndRunDrpcQueries(String[] args, String topologyAndDrpcServiceName, Config config) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, TException, DRPCExecutionException {
        long duration     = 0;
        DRPCClient client = new DRPCClient("localhost", 3772);
        StormSubmitter.submitTopology(topologyAndDrpcServiceName, config, buildTopology(null, args[0], Long.valueOf(args[1])));
        for(int i = 0; i < NUM_QUERIES; i++) {
            Thread.sleep(Long.valueOf(args[2]));
            long startTime = System.currentTimeMillis();
            System.out.println(MessageFormat.format("Result for PSP query is -> {0}",
                    client.execute(topologyAndDrpcServiceName, topologyAndDrpcServiceName)));
            long endTime = System.currentTimeMillis();
            duration += endTime - startTime;
        }
        printTimings(duration, NUM_QUERIES);
        cleanup(client);
    }

    private static final int NUM_QUERIES = 10;
}

