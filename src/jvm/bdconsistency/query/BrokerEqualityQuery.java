package jvm.bdconsistency.query;

import backtype.storm.tuple.Values;
import jvm.bdconsistency.Trade;
import jvm.bdconsistency.ask.AsksState;
import jvm.bdconsistency.bid.BidsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

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
            ArrayList<Map<Long, List<Trade>>> askTable = new ArrayList<Map<Long, List<Trade>>>();
            askTable.add(asksState.getAsks());
            return askTable;
        }

        @Override
        public void execute(TridentTuple tuple, Map<Long, List<Trade>> askTable, TridentCollector collector) {
            collector.emit(new Values(askTable));
        }
    }

    public static class AsksBidsEquiJoinByBrokerIdPredicate extends BaseQueryFunction<BidsState, HashMap<Long, Long>> {

        @Override
        public List<HashMap<Long, Long>> batchRetrieve(BidsState state, List<TridentTuple> inputs) {
            Map<Long, List<Trade>> asksTable = null, bidsTable = state.getBids();
            // We expect only one input tuple containing the asks table
            for (TridentTuple input : inputs) {
                if (input.getValueByField("asks") instanceof Map) {
                    asksTable = (Map<Long, List<Trade>>) input.getValueByField("asks");
                    break;
                }
            }
            assert (asksTable != null);

            HashMap<Long, Long> result = new HashMap<Long, Long>();
            for (long broker : bidsTable.size() > asksTable.size() ? bidsTable.keySet() : asksTable.keySet()) {
                long asksVolume = 0, asksPrice = 0, bidsVolume = 0, bidsPrice = 0;
                for (Trade ask : asksTable.get(broker)) {
                    asksVolume += ask.getVolume();
                    asksPrice += ask.getPrice();
                }
                for (Trade bid : bidsTable.get(broker)) {
                    bidsVolume += bid.getVolume();
                    bidsPrice += bid.getPrice();
                }
                if (asksPrice - bidsPrice > 1000 || bidsPrice - asksPrice > 1000)
                    result.put(broker, asksVolume - bidsVolume);
            }

            ArrayList<HashMap<Long, Long>> resultList = new ArrayList<HashMap<Long, Long>>();
            resultList.add(result);

            return resultList;
        }

        @Override
        public void execute(TridentTuple tuple, HashMap<Long, Long> result, TridentCollector collector) {
            for (Long broker : result.keySet()) {
                collector.emit(new Values(broker, result.get(broker)));
            }
        }
    }
}
