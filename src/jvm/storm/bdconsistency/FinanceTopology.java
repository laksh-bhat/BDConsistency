package bdconsistency;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import bdconsistency.ask.AsksStateFactory;
import bdconsistency.ask.AsksUpdater;
import bdconsistency.ask.FileStreamingSpout;
import bdconsistency.bid.BidsStateFactory;
import bdconsistency.bid.BidsUpdater;
import bdconsistency.query.AxFinderFilter;
import bdconsistency.query.BrokerEqualityQuery;
import bdconsistency.query.PrinterBolt;
import bdconsistency.query.QuerySpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.RichSpoutBatchExecutor;

public class FinanceTopology {

    public static void main(String[] args) throws Exception {
        final ITridentSpout asksBatchSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(args[0]));
        final ITridentSpout bidsBatchSpout = new RichSpoutBatchExecutor(new FileStreamingSpout(args[0]));

        TridentTopology topology = new TridentTopology();

        TridentState asks = topology.newStream("askSpout", asksBatchSpout)
                //.each(new Fields("tradeString"), new PrinterBolt())
                .parallelismHint(8)
                .each(new Fields("tradeString"),
                        new bdconsistency.TradeConstructor.AskTradeConstructor(),
                        new Fields("brokerId", "trade")
                ).partitionBy(new Fields("brokerId"))
                //.each(new Fields("brokerId", "trade"), new PrinterBolt())
                .partitionPersist(new AsksStateFactory(), new Fields("trade"), new AsksUpdater());

        TridentState bids = topology.newStream("bidsSpout", bidsBatchSpout)
                .parallelismHint(8)
                .each(new Fields("tradeString"),
                        new bdconsistency.TradeConstructor.BidTradeConstructor(),
                        new Fields("brokerId", "trade")
                ).partitionBy(new Fields("brokerId"))
                //.each(new Fields("brokerId", "trade"), new PrinterBolt())
                .partitionPersist(new BidsStateFactory(), new Fields("trade"), new BidsUpdater());

        //query
        {
            // This has to be done using TickTuple somehow
            Stream stream = topology.newStream("querySpout", new QuerySpout())
                    //.each(new Fields("query"), new PrinterBolt())
                    .stateQuery(asks, new Fields("query"), new MapGet()/*BrokerEqualityQuery.SelectStarFromAsks()*/, new Fields("table", "brokerId", "price", "volume"))
                    .partitionBy(new Fields("brokerId"))
                    .stateQuery(bids, new Fields("table", "brokerId", "price", "volume"), new BrokerEqualityQuery.AsksEquiJoinBidsOnBrokerIdAndGroupByBrokerId(), new Fields("broker", "volume-sum", "price-diff"))
                    .partitionBy(new Fields("brokerId"))
                    //.each(new Fields("broker", "volume-sum", "price-diff"), new AxFinderFilter.PriceBasedFilter())
                    .each(new Fields("broker", "volume-sum"), new PrinterBolt());
            stream.groupBy(new Fields("broker"));
        }

        Config conf = new Config();
        conf.setNumWorkers(20);
        conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 1000);
        conf.setMaxSpoutPending(500);
        StormSubmitter.submitTopology("FinanceTopology", conf, topology.build());
    }
/*
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
       *//* new Thread(threadName) {
            @Override
            public void run() {*//*
        try {
            Scanner scanner = new Scanner(new File(fileName));
            List<String> batchOfTuples = new ArrayList<String>();
            while (scanner.hasNextLine()) {
                if (batchOfTuples.size() >= BATCH_SIZE) {
                    System.out.println(batchOfTuples.get(0));
                    batch.feed(batchOfTuples);
                    batchOfTuples.clear();
                    try {
                        Thread.sleep(100);
                        System.out.println("Sleeping for 100 ms");
                    } catch (InterruptedException ignore) {
                    }
                }
                batchOfTuples.add(scanner.nextLine());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
*//*            }
        }.start();*//*
    }*/


}
