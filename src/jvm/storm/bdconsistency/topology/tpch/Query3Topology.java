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
import bdconsistency.bolt.trident.filter.PrinterBolt;
import bdconsistency.bolt.trident.filter.TpchFilter;
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
import java.text.MessageFormat;

import static bdconsistency.topology.TopologyBase.cleanup;
import static bdconsistency.topology.TopologyBase.printTimings;

/**
 * User: lbhat@damsl
 * Date: 11/4/13
 * Time: 11:58 AM
 */
public class Query3Topology {

    public static StormTopology buildTopology (LocalDRPC drpc, String fileName, String drpcFunctionName) {
        //final ITridentSpout agendaSpout = new TransactionalTextFileSpout("agenda", fileName, "UTF-8");
        final ITridentSpout agendaSpout = new RichSpoutBatchExecutor(new NonTransactionalFileStreamingSpout(fileName, "agenda"));
        final TridentTopology topology = new TridentTopology();

        final Stream basicStream = topology.newStream("agenda-spout", agendaSpout);
        final Stream tpchStream = basicStream
                .each(new Fields("agenda"),
                      new Split.Query3AgendaTableSplit(), new Fields("table", "orderkey", "custkey", "agendaObject"))
                .each(new Fields("table", "agendaObject"), new TpchFilter.Query3Filter(1080548553L, 19950315L, 19950315L))
                .project(new Fields("table", "orderkey", "custkey", "agendaObject"))
        ;

        // In this state we will save the tables
        TridentState tpchState = tpchStream
                .partitionBy(new Fields("orderkey", "custkey"))
                //.persistentAggregate(TpchState.FACTORY, new Fields("table", "agendaObject"), new TpchStateBuilder(), new Fields("tpchTable"))
                .partitionPersist(TpchState.FACTORY, new Fields("table", "agendaObject"), new TpchStateUpdater())
                .parallelismHint(32)
        ;

        // DRPC Query Service
        topology
                .newDRPCStream(drpcFunctionName, drpc)
                .broadcast()
                .stateQuery(tpchState,
                            new Fields("args"),
                            new TpchQuery.Query3(),
                            new Fields("orderkey", "orderdate", "shippriority", "extendedprice", "discount"))
                .parallelismHint(32)
                .groupBy(new Fields("orderkey", "orderdate", "shippriority"))
                .aggregate(new Fields("orderkey", "orderdate", "shippriority", "extendedprice", "discount")
                        , new TpchQuery.Query3Aggregator()
                        , new Fields("query3"))
                .parallelismHint(16)
                .project(new Fields("orderkey", "orderdate", "shippriority", "query3"))
        ;

        return topology.build();
    }

    public static void SubmitTopologyAndRunDrpcQueries (String[] args, String topologyAndDrpcServiceName, Config config) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, TException, DRPCExecutionException, IOException {
        StormSubmitter.submitTopology(topologyAndDrpcServiceName, config, buildTopology(null, args[0], topologyAndDrpcServiceName));
    }

    private static void saveResults (final BufferedWriter writer, final String result) throws IOException {
        writer.append(result);
        writer.newLine();
        writer.flush();
    }

    private static String runQuery (final String topologyAndDrpcServiceName, final DRPCClient client) throws TException, DRPCExecutionException {/*Query Arguments in order -- marketsegment, orderdate, shipdate*/
        return client.execute(topologyAndDrpcServiceName, "1080548553,19950315,19950315");
    }

    public static void main (String[] args) throws Exception {
        Config config = PropertiesReader.getStormConfig();
        StormSubmitter.submitTopology("Q3", config, buildTopology(null, args[0], "Q3"));
    }
}


