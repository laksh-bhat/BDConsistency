package bdconsistency.query;

import backtype.storm.tuple.Values;
import bdconsistency.trade.Trade;
import bdconsistency.ask.AsksState;
import bdconsistency.bid.BidsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 3:57 AM
 */

public class BrokerEqualityQuery {

    public static class SelectStarFromAsks extends BaseQueryFunction<AsksState, Map<Long, List<Trade>>> {
        public List<Map<Long, List<Trade>>> batchRetrieve(AsksState asksState, List<TridentTuple> inputs) {
            System.out.println(MessageFormat.format("-- SelectStarFromAsks -- {0}", asksState.getTotalTrade()));
            List<Map<Long, List<Trade>>> returnList = new ArrayList<Map<Long, List<Trade>>>(1);
            returnList.add((HashMap<Long, List<Trade>>)asksState.getAsks());
            return returnList;
        }

        @Override
        public void execute(TridentTuple tuple, Map<Long, List<Trade>> result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }

    public static class SelectStarFromBids extends BaseQueryFunction<BidsState, Map<Long, List<Trade>>> {
        public List<Map<Long, List<Trade>>> batchRetrieve(BidsState bidsState, List<TridentTuple> inputs) {
            System.out.println("-- SelectStarFromBids -- " + bidsState.getTotalTrade());

            List<Map<Long, List<Trade>>> returnList = new ArrayList<Map<Long, List<Trade>>>(1);
            returnList.add((HashMap<Long, List<Trade>>)bidsState.getBids());

            return returnList;
        }

        @Override
        public void execute(TridentTuple tuple, Map<Long, List<Trade>> result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }

    public static class AsksEquiJoinBidsOnBrokerIdAndGroupByBrokerId extends BaseQueryFunction<BidsState, List<Long>> {

        @Override
        public List<List<Long>> batchRetrieve(BidsState state, List<TridentTuple> inputs) {
            System.out.println("AsksEquiJoinBidsOnBrokerIdAndGroupByBrokerId -- " + inputs.get(0));
            Map<Long, List<Trade>> bidsTable = state.getBids();
            Map<Long, List<TridentTuple>> asksTable = new HashMap<Long, List<TridentTuple>>();
            for (TridentTuple tuple : inputs) {
                long broker = tuple.getLongByField("brokerId");
                if (!asksTable.containsKey(broker))
                    asksTable.put(broker, new ArrayList<TridentTuple>());
                asksTable.get(broker).add(tuple);
            }

            List<List<Long>> result = new ArrayList<List<Long>>(inputs.size());
            for (long broker : asksTable.keySet()) {
                long asksVolume = 0, asksPrice = 0, bidsVolume = 0, bidsPrice = 0;
                for (Object ask : asksTable.get(broker)) {
                    asksVolume += ((TridentTuple) ask).getLongByField("volume");
                    asksPrice += ((TridentTuple) ask).getLongByField("price");
                }
                for (Trade bid : bidsTable.get(broker)) {
                    bidsVolume += bid.getVolume();
                    bidsPrice += bid.getPrice();
                }
                List<Long> resultRow = new ArrayList<Long>();
                resultRow.add(broker);
                resultRow.add(asksVolume - bidsVolume);
                resultRow.add(Math.abs(asksPrice - bidsPrice));

                result.add(resultRow);
            }
            return result;
        }

        @Override
        public void execute(TridentTuple tuple, List<Long> result, TridentCollector collector) {
            if (result.get(2) > 1000)
                collector.emit(new Values(result));
        }
    }
}
