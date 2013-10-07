package bdconsistency;

import backtype.storm.tuple.Fields;
import bdconsistency.ask.AsksStateFactory;
import bdconsistency.ask.AsksUpdater;
import bdconsistency.bid.BidsStateFactory;
import bdconsistency.bid.BidsUpdater;
import bdconsistency.query.BrokerEqualityQuery;
import bdconsistency.query.PrintTuple;
import bdconsistency.query.QuerySpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.FeederCommitterBatchSpout;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class FinanceTopology {

    public static void main(String[] args) throws Exception {

        ArrayList<String> fields = new ArrayList<String>();
        fields.add("tradeString");

        final FeederBatchSpout asksBatchSpout = new FeederBatchSpout(fields);
        final FeederBatchSpout bidsBatchSpout = new FeederBatchSpout(fields);

        TridentTopology topology = new TridentTopology();

        TridentState asks = topology.newStream("askSpout", asksBatchSpout)
                //.parallelismHint(8)
                .each(new Fields("tradeString"),
                        new bdconsistency.TradeConstructor.AskTradeConstructor(),
                        new Fields("brokerId", "trade")
                )//.partitionBy(new Fields("brokerId"))
                .each(new PrintTuple(), new Fields("brokerId", "trade"))
                .partitionPersist(new AsksStateFactory(), new Fields("trade"), new AsksUpdater());

        TridentState bids = topology.newStream("bidsSpout", bidsBatchSpout)
                //.parallelismHint(8)
                .each(new Fields("tradeString"),
                        new bdconsistency.TradeConstructor.BidTradeConstructor(),
                        new Fields("brokerId", "trade")
                )//.partitionBy(new Fields("brokerId"))
                .each(new PrintTuple(), new Fields("brokerId", "trade"))
                .partitionPersist(new BidsStateFactory(), new Fields("trade"), new BidsUpdater());

        //query
        {
            // This has to be done using TickTuple somehow
            Stream stream = topology.newStream("querySpout", new QuerySpout())
                    .stateQuery(asks, new BrokerEqualityQuery.SelectStarFromAsks(), new Fields("asks"))
                    .stateQuery(bids, new BrokerEqualityQuery.AsksBidsEquiJoinByBrokerIdPredicate(), new Fields("brokerId", "volume"))
                    .each(new PrintTuple(), new Fields(""))
                    ;
        }
        assert (args[0] != null);
        startStreaming(asksBatchSpout, bidsBatchSpout, args[0], args[0]);
    }

    private static void startStreaming(FeederBatchSpout asksBatchSpout, FeederBatchSpout bidsBatchSpout, String asksFileName, String bidsFileName) {
        feedSpoutWithTradeFromFile(asksFileName, asksBatchSpout, "AsksFeeder");
        feedSpoutWithTradeFromFile(bidsFileName, bidsBatchSpout, "BidsFeeder");
    }

    private static void feedSpoutWithTradeFromFile
    (
        final String fileName,
        final FeederBatchSpout batch,
        final String threadName
    ) {
        new Thread(threadName) {
            @Override
            public void run() {
                try {
                    Scanner scanner = new Scanner(new File(fileName));
                    List<String> batchOfTuples = new ArrayList<String>();
                    while (scanner.hasNextLine()) {
                        if(batchOfTuples.size() >= BATCH_SIZE) {
                            /*System.out.println(batchOfTuples.get(0));*/
                            batch.feed(batchOfTuples);
                            batchOfTuples.clear();
                            try {
                                Thread.sleep(100);
                                System.out.println("Sleeping for 100 ms");
                            } catch (InterruptedException ignore) {}
                        }
                        batchOfTuples.add(scanner.nextLine());
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }


    private static final int BATCH_SIZE = 10;
}
