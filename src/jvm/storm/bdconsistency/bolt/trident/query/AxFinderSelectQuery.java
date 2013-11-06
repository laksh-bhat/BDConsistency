package bdconsistency.bolt.trident.query;

import backtype.storm.tuple.Values;
import bdconsistency.trade.Trade;
import bdconsistency.state.ask.AsksState;
import bdconsistency.state.bid.BidsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 3:57 AM
 */

public class AxFinderSelectQuery {

    public static class SelectStarFromAsks extends BaseQueryFunction<AsksState, Map<Long, List<Trade>>> {
        public List<Map<Long, List<Trade>>> batchRetrieve(AsksState asksState, List<TridentTuple> inputs) {
            Map<Long, List<Trade>> concurrentMap = new ConcurrentHashMap<Long, List<Trade>>();
            for (long key : asksState.getAsksHashMapBackedTable().keySet()){
                Trade t = asksState.getTradeByKey(key);
                if(t == null) continue;
                if(!concurrentMap.containsKey(t.getBrokerId()))
                    concurrentMap.put(t.getBrokerId(), new ArrayList<Trade>());

                concurrentMap.get(t.getBrokerId()).add(t);
            }

            List<Map<Long, List<Trade>>> batch = new ArrayList<Map<Long, List<Trade>>>(1);
            batch.add(concurrentMap);
            for (int i = 1; i < inputs.size(); i++) {
                batch.add(null);
            }
            return batch;
        }

        @Override
        public void execute(TridentTuple tuple, Map<Long, List<Trade>> result, TridentCollector collector) {
            if(null != result)
                collector.emit(new Values(result));
        }
    }

    public static class SelectStarFromBids extends BaseQueryFunction<BidsState, Map<Long, List<Trade>>> {
        public List<Map<Long, List<Trade>>> batchRetrieve(BidsState bidsState, List<TridentTuple> inputs) {
            Map<Long, List<Trade>> concurrentMap = new ConcurrentHashMap<Long, List<Trade>>();
            for (long key : bidsState.getBidsHashMapBackedTable().keySet()){
                Trade t = bidsState.getTradeByKey(key);
                if(t == null) continue;
                if(!concurrentMap.containsKey(t.getBrokerId()))
                    concurrentMap.put(t.getBrokerId(), new ArrayList<Trade>());

                concurrentMap.get(t.getBrokerId()).add(t);
            }

            List<Map<Long, List<Trade>>> batch = new ArrayList<Map<Long, List<Trade>>>(1);
            batch.add(concurrentMap);
            for (int i = 1; i < inputs.size(); i++) {
                batch.add(concurrentMap);
            }
            return batch;
        }

        @Override
        public void execute(TridentTuple tuple, Map<Long, List<Trade>> result, TridentCollector collector) {
            if(null != result)
                collector.emit(new Values(result));
        }
    }
}
