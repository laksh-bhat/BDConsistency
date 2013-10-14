package bdconsistency;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DRPCClient;
import bdconsistency.ask.AsksStateFactory;
import bdconsistency.ask.AsksUpdater;
import bdconsistency.bid.BidsStateFactory;
import bdconsistency.bid.BidsUpdater;
import bdconsistency.query.*;
import com.google.common.collect.Lists;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.StateUpdater;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FinanceTopology {
    public static class CountUpdater implements StateUpdater<CounterState> {
        @Override
        public void updateState(CounterState state, List<TridentTuple> tuples, TridentCollector collector) {
            for (TridentTuple t : tuples)
                state.increment();
        }
        @Override
        public void prepare(Map conf, TridentOperationContext context) {}

        @Override
        public void cleanup() {}
    }

    public static StormTopology buildTopology(LocalDRPC drpc, String fileName, long statesize) {
        TridentTopology topology = new TridentTopology();
        final ITridentSpout asksSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));
        final ITridentSpout bidsSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));

        // In this state we will save the table
        Stream asksStream = topology
                .newStream("spout1", asksSpout);
        Stream bidsStream = topology
                .newStream("spout2", bidsSpout);

        TridentState asks =  asksStream.each(new Fields("tradeString"), new AxFinderFilter.AsksFilter())
                .shuffle()
                .parallelismHint(8)
                //.each(new Fields("tradeString"), new PrinterBolt())
                .partitionPersist(new AsksStateFactory(statesize), new Fields("tradeString"), new AsksUpdater())
                .parallelismHint(8);

        TridentState bids = bidsStream
                .each(new Fields("tradeString"), new AxFinderFilter.BidsFilter())
                .shuffle()
                .parallelismHint(8)
                //.each(new Fields("tradeString"), new PrinterBolt())
                .partitionPersist(new BidsStateFactory(statesize), new Fields("tradeString"), new BidsUpdater())
                .parallelismHint(8)
                ;

        TridentState count =
        topology.merge(asksStream, bidsStream)
                .shuffle()
                .partitionPersist(new CounterState.CounterStateFactory(), new Fields("tradeString"), new CountUpdater())
                ;

        // DRPC Service
        topology
                .newDRPCStream("AXF")
                .each(new Fields("args"), new PrinterBolt())
                .shuffle()
                .stateQuery(asks, new BrokerEqualityQuery.SelectStarFromAsks(), new Fields("asks"))
                .parallelismHint(5)
                .shuffle()
                .stateQuery(bids, new BrokerEqualityQuery.SelectStarFromBids(), new Fields("bids"))
                .parallelismHint(5)
                .each(new Fields("asks", "bids"), new PrinterBolt())
                .shuffle()
                .each(new Fields("asks", "bids"), new AsksBidsJoin(), new Fields("AXF"))
                .shuffle()
                //.parallelismHint(5)
                .project(new Fields("AXF"))
                .shuffle()
                .stateQuery(count, new BaseQueryFunction<CounterState, Object>() {
                    @Override
                    public List<Object> batchRetrieve(CounterState state, List<TridentTuple> args) {
                        List<Object> returnList = new ArrayList<Object>();
                        returnList.add(state.getCount());
                        return returnList;
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
        Config conf = new Config();
        conf.setNumWorkers(20);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("damsel", "qp4", "qp5", "qp6"));
        conf.setMaxSpoutPending(20);
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        StormSubmitter.submitTopology("AXFinder", conf, buildTopology(null, args[0], args[1] != null? Long.valueOf(args[1]) : 100000));
        Thread.sleep(10000);

        DRPCClient client = new DRPCClient("localhost", 3772);
        // Fire AXFinder Query 100 times
        long duration = 0;
        for(int i = 0; i < 10; i++) {
            Thread.sleep(30000);
            long startTime = System.currentTimeMillis();
            System.out.println("Result for AXF query is -> " + client.execute("AXF", "axfinder"));
            long endTime = System.currentTimeMillis();
            duration += endTime - startTime;
        }

        System.out.println("==================================================================");
        System.out.println(MessageFormat.format("duration for 50 ax-finder queries {0} mill seconds", duration));
        client.close();
    }
}
