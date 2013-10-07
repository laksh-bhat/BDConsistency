package jvm.bdconsistency;

import backtype.storm.tuple.Fields;
import jvm.bdconsistency.ask.AsksStateFactory;
import jvm.bdconsistency.ask.AsksUpdater;
import jvm.bdconsistency.bid.BidsStateFactory;
import jvm.bdconsistency.bid.BidsUpdater;
import jvm.bdconsistency.query.BrokerEqualityQuery;
import jvm.bdconsistency.query.PrintTuple;
import jvm.bdconsistency.query.QuerySpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.FeederCommitterBatchSpout;
import storm.trident.testing.FixedBatchSpout;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class FinanceTopology {

    public static void main(String[] args) throws Exception {

        ArrayList<String> fields = new ArrayList<String>();
        fields.add("financialTransaction");

        final FeederCommitterBatchSpout asksBatchSpout = new FeederCommitterBatchSpout(fields);
        final FeederCommitterBatchSpout bidsBatchSpout = new FeederCommitterBatchSpout(fields);

        TridentTopology topology = new TridentTopology();

        TridentState asks = topology.newStream("askSpout", asksBatchSpout)
                .parallelismHint(16)
                .each(new Fields("financialTransaction"),
                        new TradeConstructor(),
                        new Fields("trade")
                ).partitionBy(new Fields("brokerId"))
                .partitionPersist(new AsksStateFactory(), new Fields("trade"), new AsksUpdater());

        TridentState bids = topology.newStream("bidsSpout", bidsBatchSpout)
                .parallelismHint(8)
                .each(new Fields("financialTransaction"),
                        new TradeConstructor(),
                        new Fields("trade")
                ).partitionBy(new Fields("brokerId"))
                .partitionPersist(new BidsStateFactory(), new Fields("trade"), new BidsUpdater());

        //query
        {
            // This has to be done using ticktuple somehow
            List<Object> queries = new ArrayList<Object>();
            queries.add("broker-equi-join");
            Stream stream = topology.newStream("querySpout", new QuerySpout())
                    .stateQuery(asks, new BrokerEqualityQuery.SelectStarFromAsks(), new Fields("asks"))
                    .stateQuery(bids, new BrokerEqualityQuery.AsksBidsEquiJoinByBrokerIdPredicate(), new Fields("broker", "volume"))
                    .each(new PrintTuple(), new Fields(""))
                    ;
        }

        startStreaming(asksBatchSpout, bidsBatchSpout);
    }

    private static void startStreaming(FeederCommitterBatchSpout asksBatchSpout, FeederCommitterBatchSpout bidsBatchSpout) {
        feedSpoutWithTradeFromFile("asks-fileName", asksBatchSpout, "AsksFeeder");
        feedSpoutWithTradeFromFile("bids-fileName", bidsBatchSpout, "BidsFeeder");
    }

    private static void feedSpoutWithTradeFromFile
    (
        final String fileName,
        final FeederCommitterBatchSpout batch,
        final String threadName
    ) {
        new Thread(threadName) {
            @Override
            public void run() {
                try {
                    Scanner scanner = new Scanner(new File(fileName));
                    List<String> batchOfTuples = new ArrayList<String>();
                    while (scanner.hasNext()) {
                        for (int i = 0; i < BATCH_SIZE; i++) {
                            batchOfTuples.add(scanner.next());
                        }
                        batch.feed(batchOfTuples);
                        batchOfTuples.clear();
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }


    private static final int BATCH_SIZE = 10;
}
