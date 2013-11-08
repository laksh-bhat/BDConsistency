package bdconsistency.topology.tpch;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DRPCClient;
import bdconsistency.bolt.trident.basefunction.Split;
import bdconsistency.bolt.trident.query.TpchQuery;
import bdconsistency.spouts.NonTransactionalFileStreamingSpout;
import bdconsistency.spouts.TransactionalTextFileSpout;
import bdconsistency.state.counter.CounterState;
import bdconsistency.state.tpch.TpchState;
import bdconsistency.state.tpch.TpchStateUpdater;
import bdconsistency.topology.finance.FinanceTopology;
import bdconsistency.utils.PropertiesReader;
import org.apache.thrift7.TException;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import static bdconsistency.topology.TopologyBase.cleanup;
import static bdconsistency.topology.TopologyBase.printTimings;

/**
 * User: lbhat@damsl
 * Date: 11/4/13
 * Time: 11:58 AM
 */
public class Query3Topology {

    public static StormTopology buildTopology (LocalDRPC drpc, String fileName, String drpcFunctionName) {
        final ITridentSpout agendaSpout = new TransactionalTextFileSpout("agenda", fileName, "UTF-8");
        //final ITridentSpout agendaSpout = new RichSpoutBatchExecutor(new NonTransactionalFileStreamingSpout(fileName, "agenda"));
        final TridentTopology topology = new TridentTopology();

        final Stream basicStream = topology.newStream("agenda-spout", agendaSpout);
        final Stream tpchStream = basicStream
                .each(new Fields("agenda"),
                      new Split.AgendaTableSplit(), new Fields("table", "orderkey", "custkey", "agendaObject"))
                .project(new Fields("table", "orderkey", "custkey", "agendaObject"));

        // In this state we will save the tables
        TridentState tpchState = tpchStream
                .partitionBy(new Fields("orderkey", "custkey"))
                //.persistentAggregate(TpchState.FACTORY, new Fields("table", "agendaObject"), new TpchStateBuilder(), new Fields("tpchTable"))
                .partitionPersist(TpchState.FACTORY, new Fields("agendaObject"), new TpchStateUpdater())
                .parallelismHint(8);


        // DRPC Service
        topology
                .newDRPCStream(drpcFunctionName, drpc)
                .broadcast()
                .stateQuery(tpchState,
                            new Fields("args"),
                            new TpchQuery.Query3(),
                            new Fields("orderkey", "orderdate", "shippriority", "extendedprice", "discount"))
                .parallelismHint(8)
                .groupBy(new Fields("orderkey", "orderdate", "shippriority"))
                .aggregate(new Fields("orderkey", "orderdate", "shippriority", "extendedprice", "discount")
                        , new TpchQuery.Query3Aggregator()
                        , new Fields("query3"))
        ;

        return topology.build();
    }

    public static void main (String[] args) throws Exception {
        Config config = PropertiesReader.getStormConfig();
        config.setDebug(true);
        SubmitTopologyAndRunDrpcQueries(args, "Q3", config);
    }

    public static void SubmitTopologyAndRunDrpcQueries (String[] args, String topologyAndDrpcServiceName, Config config) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, TException, DRPCExecutionException {
        long duration = 0;
        DRPCClient client = new DRPCClient("qp-hd1", 3772);
        StormSubmitter.submitTopology(topologyAndDrpcServiceName, config, buildTopology(null, args[0], topologyAndDrpcServiceName));
        for (int i = 0; i < NUM_QUERIES; i++) {
            Thread.sleep(20000);
            long startTime = System.currentTimeMillis();
            System.out.println(MessageFormat.format("Result for Q3 query is -> {0}",
                                                    client.execute(topologyAndDrpcServiceName, "1080548553,19960327,19960400" /*Query Arguments in order -- marketsegment, orderdate, shipdate*/)));
            long endTime = System.currentTimeMillis();
            duration += endTime - startTime;
        }
        printTimings(duration, NUM_QUERIES);
        cleanup(client);
    }

    private static final int NUM_QUERIES = 10;
}


