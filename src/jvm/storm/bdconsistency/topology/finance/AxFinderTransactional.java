package bdconsistency.topology.finance;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
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
import bdconsistency.bolt.trident.query.AxFinderAsksBidsEquiJoinOnBrokerId;
import bdconsistency.bolt.trident.query.AxFinderSelectQuery;
import bdconsistency.spouts.TransactionalTextFileSpout;
import bdconsistency.utils.PropertiesReader;
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

/**
 * User: lbhat@damsl
 * Date: 10/29/13
 * Time: 2:00 PM
 */
public class AxFinderTransactional {
        public static class CountUpdater implements StateUpdater<CounterState> {
            @Override
            public void updateState(CounterState state, List<TridentTuple> tuples, TridentCollector collector) {
                for (TridentTuple tuple : tuples) {
                    state.increment();
                }
            }
            @Override
            public void prepare(Map conf, TridentOperationContext context) {}
            @Override
            public void cleanup() {}
        }

        public static StormTopology buildTopology(LocalDRPC drpc, String fileName, long stateSize) {
            TridentTopology topology      = new TridentTopology();
            final ITridentSpout asksSpout = new TransactionalTextFileSpout("tradeString", fileName, "UTF-8");
            final ITridentSpout bidsSpout = new TransactionalTextFileSpout("tradeString", fileName, "UTF-8");
            //final ITridentSpout bidsSpout = new RichSpoutBatchExecutor(new NonTransactionalFileStreamingSpout(fileName));

            // In this state we will save the table
            Stream asksStream = topology
                    .newStream("spout1", asksSpout);
            Stream bidsStream = topology
                    .newStream("spout2", bidsSpout);

            TridentState asks =  asksStream.each(new Fields("tradeString"), new AxFinderFilter.AsksFilter())
                    .shuffle()
                    .parallelismHint(8)
                    .partitionPersist(new AsksStateFactory(stateSize), new Fields("tradeString"), new AsksUpdater())
                    .parallelismHint(8);

            TridentState bids = bidsStream
                    .each(new Fields("tradeString"), new AxFinderFilter.BidsFilter())
                    .shuffle()
                    .parallelismHint(8)
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
                    .newDRPCStream("AXF", drpc)
                    .each(new Fields("args"), new PrinterBolt())
                    .shuffle()
                    .stateQuery(asks, new AxFinderSelectQuery.SelectStarFromAsks(), new Fields("asks"))
                    .parallelismHint(5)
                    .shuffle()
                    .stateQuery(bids, new AxFinderSelectQuery.SelectStarFromBids(), new Fields("bids"))
                    .parallelismHint(5)
                    .each(new Fields("asks", "bids"), new PrinterBolt())
                    .shuffle()
                    .each(new Fields("asks", "bids"), new AxFinderAsksBidsEquiJoinOnBrokerId(), new Fields("AXF"))
                    .shuffle()
                            //.parallelismHint(5)
                    .project(new Fields("AXF"))
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
            Config conf = PropertiesReader.getStormConfig();
            StormSubmitter.submitTopology("AXF", conf, buildTopology(null, args[0], Long.valueOf(args[1])));
            Thread.sleep(10000);

            DRPCClient client = new DRPCClient("localhost", 3772);
            // Fire AXFinder Query 100 times
            long duration = 0;
            for(int i = 0; i < NUM_QUERIES; i++) {
                Thread.sleep(Long.valueOf(args[2]));
                long startTime = System.currentTimeMillis();
                System.out.println(MessageFormat.format("Result for AXF query is -> {0}", client.execute("AXF", "axfinder")));
                long endTime = System.currentTimeMillis();
                duration += endTime - startTime;
            }

            System.out.println("=======================================================================");
            System.out.println(MessageFormat.format("duration for {1} ax-finder queries {0} mill seconds",
                    duration, NUM_QUERIES));
            client.close();
        }
        private static final int NUM_QUERIES = 10;
}

