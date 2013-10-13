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
                .shuffle()
                .each(new Fields("tradeString"), new AxFinderFilter.AsksFilter())
                .parallelismHint(8)
                //.each(new Fields("tradeString"), new PrinterBolt())
                .partitionPersist(new AsksStateFactory(), new Fields("tradeString"), new AsksUpdater())
                .parallelismHint(8);

        TridentState bids = topology
                .newStream("spout2", bidsSpout)
                .shuffle()
                .each(new Fields("tradeString"), new AxFinderFilter.BidsFilter())
                .parallelismHint(8)
                //.each(new Fields("tradeString"), new PrinterBolt())
                .shuffle()
                .partitionPersist(new BidsStateFactory(), new Fields("tradeString"), new BidsUpdater())
                .parallelismHint(8)
                ;

        // DRPC Service
        topology
                .newDRPCStream("AXF")
                .shuffle()
                .stateQuery(asks, new BrokerEqualityQuery.SelectStarFromAsks(), new Fields("asks"))
                .parallelismHint(8)
                .shuffle()
                .stateQuery(bids, new BrokerEqualityQuery.SelectStarFromBids(), new Fields("bids"))
                .parallelismHint(8)
                .shuffle()
                .each(new Fields("asks", "bids"), new AsksBidsJoin(), new Fields("AXF"))
                .shuffle()
                .parallelismHint(8)
                .project(new Fields("AXF"));

        return topology.build();
    }


    public static void main(String[] args) throws Exception {
        Config topologyConfig = new Config();
        topologyConfig.put(Config.DRPC_SERVERS, Lists.newArrayList("damsel", "qp4", "qp5", "qp6"));
        topologyConfig.setMaxSpoutPending(4);
        topologyConfig.put(Config.STORM_CLUSTER_MODE, "distributed");
        StormSubmitter.submitTopology("AXFinder", topologyConfig, buildTopology(null, args[0]));
        // Let it run for 5 minutes
        Thread.sleep(300000);
        DRPCClient client = new DRPCClient("damsel", 3772);
        System.out.println("Result for AXF query is -> " + client.execute("AXF", "None"));
    }
}
