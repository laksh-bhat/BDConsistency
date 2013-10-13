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
                .partitionPersist(new BidsStateFactory(), new Fields("tradeString"), new BidsUpdater())
                ;

        // DRPC Service
        topology
                .newDRPCStream("AXF")
                //.shuffle()
                .stateQuery(asks, new BrokerEqualityQuery.SelectStarFromAsks(), new Fields("asks"))
                .parallelismHint(8)
                .shuffle()
                .stateQuery(bids, new BrokerEqualityQuery.SelectStarFromBids(), new Fields("bids"))
                //.parallelismHint(8)
                //.shuffle()
                .each(new Fields("asks", "bids"), new AsksBidsJoin(), new Fields("AXF"))
                .shuffle()
                .parallelismHint(8)
                .project(new Fields("AXF"));

        return topology.build();
    }


    public static void main(String[] args) throws Exception {
        Config topologyConfig = new Config();
        topologyConfig.setNumWorkers(20);
        topologyConfig.put(Config.DRPC_SERVERS, Lists.newArrayList("localhost"));
        topologyConfig.setMaxSpoutPending(4);
        topologyConfig.put(Config.STORM_CLUSTER_MODE, "distributed");
        StormSubmitter.submitTopology("AXFinder", topologyConfig, buildTopology(null, args[0]));
        // Let it run for 5 minutes
        Thread.sleep(10000);
        DRPCClient client = new DRPCClient("localhost", 3772);
        for (int i = 0 ; i < 50 ; i++){
            System.out.println("Result for AXF query is -> " + client.execute("AXF", "None"));
            Thread.sleep(1000);
        }
        client.close();
    }
}
