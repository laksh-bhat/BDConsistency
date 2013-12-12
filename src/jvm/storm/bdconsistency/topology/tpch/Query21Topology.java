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
import bdconsistency.state.tpch.TpchState;
import bdconsistency.state.tpch.TpchStateUpdater;
import bdconsistency.utils.PropertiesReader;
import org.apache.thrift7.TException;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import static bdconsistency.topology.TopologyBase.cleanup;
import static bdconsistency.topology.TopologyBase.printTimings;

/**
 * User: lbhat@damsl
 * Date: 11/4/13
 * Time: 11:58 AM
 */
public class Query21Topology {

    public static StormTopology buildTopology (LocalDRPC drpc, String fileName, String drpcFunctionName) {
        //final ITridentSpout agendaSpout = new TransactionalTextFileSpout("agenda", fileName, "UTF-8");
        final ITridentSpout agendaSpout = new RichSpoutBatchExecutor(new NonTransactionalFileStreamingSpout(fileName, "agenda"));
        final TridentTopology topology = new TridentTopology();

        final Stream basicStream = topology.newStream("agenda-spout", agendaSpout);
        final Stream tpchStream = basicStream
                .each(new Fields("agenda"),
                      new Split.Query21AgendaTableSplit(), new Fields("table", "orderkey", "supplierkey", "nationkey", "agendaObject"))
                .project(new Fields("table", "orderkey", "supplierkey", "nationkey", "agendaObject"))
                ;

        // In this state we will save the tables
        TridentState tpchState = tpchStream
                .partitionBy(new Fields("orderkey", "nationkey"))
                        //.persistentAggregate(TpchState.FACTORY, new Fields("table", "agendaObject"), new TpchStateBuilder(), new Fields("tpchTable"))
                .partitionPersist(TpchState.FACTORY, new Fields("table", "agendaObject"), new TpchStateUpdater())
                .parallelismHint(16)
                ;

        // DRPC Query Service
        topology
                .newDRPCStream(drpcFunctionName, drpc)
                .broadcast()
                .stateQuery(tpchState,
                            new Fields("args"),
                            new TpchQuery.Query21(),
                            new Fields("suppliername"))
                .parallelismHint(16)
                .groupBy(new Fields("suppliername"))
                .aggregate(new Fields("suppliername")
                        , new TpchQuery.Query21Aggregator()
                        , new Fields("suppliername", "count"))
                .parallelismHint(16)
        ;

        return topology.build();
    }

    public static void main (String[] args) throws Exception {
        Config config = PropertiesReader.getStormConfig();
        SubmitTopologyAndRunDrpcQueries(args, "Q21", config);
    }

    public static void SubmitTopologyAndRunDrpcQueries (String[] args, String topologyAndDrpcServiceName, Config config) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, TException, DRPCExecutionException, IOException {
        long duration = 0;
        BufferedWriter writer = new BufferedWriter(new FileWriter("query-result-21.dat", false /*append*/));
        DRPCClient client = new DRPCClient("localhost", 3772);
        try {
            StormSubmitter.submitTopology(topologyAndDrpcServiceName, config, buildTopology(null, args[0], topologyAndDrpcServiceName));
            Thread.sleep(120000);

            for (int i = 0; i < NUM_QUERIES; i++) {
                long startTime = System.currentTimeMillis();
                String result = runQuery(topologyAndDrpcServiceName, client);
                saveResults(writer, result);
                long endTime = System.currentTimeMillis();
                duration += endTime - startTime;
                Thread.sleep(60000);
            }
        } finally {
            cleanup(client, writer);
        }
        printTimings(duration, NUM_QUERIES);
    }

    private static void saveResults (final BufferedWriter writer, final String result) throws IOException {
        writer.append(result);
        writer.newLine();
        System.err.println("Debug: Saved Results!");
    }

    private static String runQuery (final String topologyAndDrpcServiceName, final DRPCClient client) throws TException, DRPCExecutionException {/*Query Arguments in order -- marketsegment, orderdate, shipdate*/
        return client.execute(topologyAndDrpcServiceName, "");
    }

    private static final int NUM_QUERIES = 5;
}


