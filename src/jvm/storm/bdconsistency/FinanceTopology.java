package bdconsistency;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import bdconsistency.ask.AsksUpdater;
import bdconsistency.ask.FileStreamingSpout;
import bdconsistency.bid.BidsUpdater;
import bdconsistency.query.*;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;

public class FinanceTopology {

    public static StormTopology buildTopology(LocalDRPC drpc, String fileName) {
        TridentTopology topology = new TridentTopology();
        final ITridentSpout asksSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));
        final ITridentSpout bidsSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));

        // In this state we will save the table
        StateFactory askStateFactory = new MemoryMapState.Factory();
        StateFactory bidStateFactory = new MemoryMapState.Factory();

        TridentState asks = topology
                .newStream("spout1", asksSpout)
                .each(new Fields("tradeString"), new PrinterBolt())
                .each(new Fields("tradeString"),
                        new TradeConstructor.AskTradeConstructor(),
                        new Fields("brokerId", "trade")
                )
                .partitionPersist(askStateFactory, new Fields("trade"), new AsksUpdater());

        TridentState bids = topology
                .newStream("spout2", bidsSpout)
                .each(new Fields("tradeString"), new PrinterBolt())
                .each(new Fields("tradeString"),
                        new TradeConstructor.BidTradeConstructor(),
                        new Fields("brokerId", "trade")
                )
                .partitionPersist(bidStateFactory, new Fields("trade"), new BidsUpdater());

        // DRPC Service

        topology
                .newDRPCStream("AXF", drpc)
                .each(new Fields("args"), new PrinterBolt())
                .stateQuery(asks, new Fields("args"), new BrokerEqualityQuery.SelectStarFromAsks(), new Fields("asks"))
                .stateQuery(bids, new Fields("args"), new BrokerEqualityQuery.SelectStarFromBids(), new Fields("bids"))

                .each(new Fields("asks", "bids"), new AsksBidsJoin(), new Fields("broker", "volume"))
                // Project allows us to keep only the interesting results
                .each(new Fields("asks", "bids"), new PrinterBolt())
                ;

        return topology.build();
    }


    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        LocalDRPC drpc = new LocalDRPC();
        //LocalCluster cluster = new LocalCluster();
        StormSubmitter.submitTopology("AXFinder", conf, buildTopology(drpc, args[0]));
        // Query 100 times for
        for(int i = 0; i < 100; i++) {
            Thread.sleep(1000);
            System.out.println("Result for AXF query is -> " + drpc.execute("AXF", "sfo"));
        }
    }
}
