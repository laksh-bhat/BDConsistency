package bdconsistency.query;

import backtype.storm.tuple.Values;
import bdconsistency.Trade;
import bdconsistency.ask.AsksState;
import bdconsistency.bid.BidsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;

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
    public static class SelectStarFromAsks extends BaseQueryFunction<AsksState, Trade> {
        public List<Trade> batchRetrieve(AsksState asksState, List<TridentTuple> inputs) {
            ArrayList<Trade> askTable = new ArrayList<Trade>();
            for (List<Trade> l : asksState.getAsks().values())
                for (Trade t : l)
                    askTable.add(t);
            return askTable;
        }

        @Override
        public void execute(TridentTuple tuple, Trade result, TridentCollector collector) {
            collector.emit(new Values(result.getTable(), result.getBrokerId(), result.getPrice(), result.getVolume()));
        }
    }

    public static class AsksBidsEquiJoinByBrokerIdPredicate extends BaseQueryFunction<BidsState, List<Long>> {

        @Override
        public List<List<Long>> batchRetrieve(BidsState state, List<TridentTuple> inputs) {
            Map<Long, List<Trade>> bidsTable = state.getBids();
            Map<Long, TridentTuple> asksTable = new HashMap<Long, TridentTuple>();
            for (TridentTuple tuple : inputs) {
                long broker  = tuple.getLongByField("brokerId");
                asksTable.put(broker, tuple);
            }
            List<List<Long>> result = new ArrayList<List<Long>>();
            for (long broker : bidsTable.size() < asksTable.size() ? bidsTable.keySet() : asksTable.keySet()) {
                long asksVolume = 0, asksPrice = 0, bidsVolume = 0, bidsPrice = 0;
                for (Object ask : asksTable.get(broker)) {
                    asksVolume += ((TridentTuple)ask).getLongByField("volume");
                    asksPrice += ((TridentTuple)ask).getLongByField("price");
                }
                for (Trade bid : bidsTable.get(broker)){
                    bidsVolume += bid.getVolume();
                    bidsPrice += bid.getPrice();
                }
                if (asksPrice - bidsPrice > 1000 || bidsPrice - asksPrice > 1000){
                    List<Long> resultRow = new ArrayList<Long>();
                    resultRow.add(broker);
                    resultRow.add(asksVolume - bidsVolume);

                    result.add(resultRow);
                }
            }
            return result;
        }

        @Override
        public void execute(TridentTuple tuple, List<Long> result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }
}
