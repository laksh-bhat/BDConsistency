package bdconsistency;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DRPCClient;
import bdconsistency.ask.AsksStateFactory;
import bdconsistency.ask.AsksUpdater;
import bdconsistency.bid.BidsStateFactory;
import bdconsistency.bid.BidsUpdater;
import bdconsistency.query.*;
import com.google.common.collect.Lists;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.testing.MemoryMapState;

public class FinanceTopology {

    public static StormTopology buildTopology(LocalDRPC drpc, String fileName) {
        TridentTopology topology = new TridentTopology();
        final ITridentSpout asksSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));
        final ITridentSpout bidsSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(fileName));

        // In this state we will save the table
        TridentState asks = topology
                .newStream("spout1", asksSpout)
                .each(new Fields("tradeString"), new AxFinderFilter.AsksFilter())
                //.each(new Fields("tradeString"), new PrinterBolt())
                .partitionPersist(new AsksStateFactory(), new Fields("tradeString"), new AsksUpdater());

        TridentState bids = topology
                .newStream("spout2", bidsSpout)
                .each(new Fields("tradeString"), new AxFinderFilter.BidsFilter())
                //.each(new Fields("tradeString"), new PrinterBolt())
                .partitionPersist(new BidsStateFactory(), new Fields("tradeString"), new BidsUpdater());

        // DRPC Service
        topology
                .newDRPCStream("AXF")
                .each(new Fields("args"), new PrinterBolt())
                //.shuffle()
                .stateQuery(asks, new BrokerEqualityQuery.SelectStarFromAsks(), new Fields("asks"))
                //.parallelismHint(5)
                .shuffle()
                .stateQuery(bids, new BrokerEqualityQuery.SelectStarFromBids(), new Fields("bids"))
                //.parallelismHint(5)
                .each(new Fields("asks", "bids"), new PrinterBolt())
                .each(new Fields("asks", "bids"), new AsksBidsJoin(), new Fields("broker", "volume"))
                .shuffle()
                .parallelismHint(5)
                .project(new Fields("broker", "volume"));

        return topology.build();
    }


    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        //conf.setDebug(true);

/*        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("AXFinder", conf, buildTopology(drpc, args[0]));
        Thread.sleep(2000);

        System.out.println("DRPC RESULTS ==> : " + drpc.execute("AXF", "axfinder"));

        cluster.shutdown();
        drpc.shutdown();*/


        //conf.setMaxSpoutPending(2);
        conf.put(Config.DRPC_SERVERS, Lists.newArrayList("localhost"));
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");

        LocalDRPC drpc = new LocalDRPC();
        StormSubmitter.submitTopology("AXFinder", conf, buildTopology(drpc, args[0]));
        Thread.sleep(1000);
        // Fire AXFinder Query 100 times
        for(int i = 0; i < 100; i++) {
            System.out.println("Result for AXF query is -> " + drpc.execute("AXF", "axfinder"));
            Thread.sleep(100);
        }
        drpc.shutdown();
    }
}
