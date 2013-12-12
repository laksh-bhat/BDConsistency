package bdconsistency.bolt.trident.query;

import backtype.storm.tuple.Values;
import bdconsistency.trade.Trade;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 10/9/13
 * Time: 6:48 PM
 */
public class AxFinderAsksBidsEquiJoinOnBrokerId extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
<<<<<<< HEAD:src/jvm/storm/bdconsistency/bolt/trident/query/AxFinderAsksBidsEquiJoinOnBrokerId.java
        System.out.println(" -- AxFinderAsksBidsEquiJoinOnBrokerId -- ");
=======
>>>>>>> master:src/jvm/storm/bdconsistency/query/AsksBidsJoin.java

        Map<Long, List<Trade>> asksTable = (Map<Long, List<Trade>>) tuple.getValueByField("asks");
        Map<Long, List<Trade>> bidsTable = (Map<Long, List<Trade>>) tuple.getValueByField("bids");

<<<<<<< HEAD:src/jvm/storm/bdconsistency/bolt/trident/query/AxFinderAsksBidsEquiJoinOnBrokerId.java
        final Set<Long> keys = Collections.synchronizedSet(asksTable.keySet());

        for (long broker : keys) {
=======
        System.out.println(
                MessageFormat.format(" -- AsksBidsJoin -- asks count -- {0}bids count -- {1}",
                        asksTable.size(), bidsTable.size()));

        for (long broker : asksTable.keySet()) {
>>>>>>> master:src/jvm/storm/bdconsistency/query/AsksBidsJoin.java
            long asksTotalVolume = 0, asksPrice = 0, bidsTotalVolume = 0, bidsPrice = 0;

            for (Trade ask : asksTable.get(broker)) {
                asksTotalVolume += ask.getVolume();
                asksPrice += ask.getPrice();
            }
            for (Trade bid : bidsTable.get(broker)) {
                bidsTotalVolume += bid.getVolume();
                bidsPrice += bid.getPrice();
            }


            if (Math.abs(bidsPrice - asksPrice) > 1000) {
                List<Long> axf = new ArrayList<Long>();
                axf.add(broker);
                axf.add(asksTotalVolume - bidsTotalVolume);
                collector.emit(new Values(axf));
            }
        }
    }
}
