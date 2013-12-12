package bdconsistency.topology.ml;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import bdconsistency.bolt.trident.basefunction.Split;
import bdconsistency.bolt.trident.filter.TpchFilter;
import bdconsistency.bolt.trident.query.TpchQuery;
import bdconsistency.spouts.NonTransactionalFileStreamingSpout;
import bdconsistency.state.tpch.TpchState;
import bdconsistency.state.tpch.TpchStateUpdater;
import bdconsistency.utils.PropertiesReader;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * User: lbhat@damsl
 * Date: 12/4/13
 * Time: 5:45 PM
 */
public class PrincipalComponentAnalyzer {
    public static StormTopology buildTopology (LocalDRPC drpc, String fileName, String drpcFunctionName) {
        final ITridentSpout sensorSpout = new RichSpoutBatchExecutor(new NonTransactionalFileStreamingSpout(fileName, "agenda"));
        final TridentTopology topology = new TridentTopology();

        final Stream sensorStream = topology.newStream("pca-data", sensorSpout);
        final Stream tpchStream = sensorStream
                .each(new Fields("agenda"),
                      new Split.Query3AgendaTableSplit(), new Fields("table", "orderkey", "custkey", "agendaObject"))
                .each(new Fields("table", "agendaObject"), new TpchFilter.Query3Filter(1080548553L, 19950315L, 19950315L))
                .project(new Fields("table", "orderkey", "custkey", "agendaObject"));

        // In this state we will save the tables
        TridentState tpchState = tpchStream
                .partitionBy(new Fields("orderkey", "custkey"))
                        //.persistentAggregate(TpchState.FACTORY, new Fields("table", "agendaObject"), new TpchStateBuilder(), new Fields("tpchTable"))
                .partitionPersist(TpchState.FACTORY, new Fields("table", "agendaObject"), new TpchStateUpdater())
                .parallelismHint(32);

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

    private static void saveResults (final BufferedWriter writer, final String result) throws IOException {
        writer.append(result);
        writer.newLine();
        writer.flush();
    }

    public static void main (String[] args) throws Exception {
        Config config = PropertiesReader.getStormConfig();
        StormSubmitter.submitTopology("PCA", config, buildTopology(null, args[0], "PCA"));
    }
}
