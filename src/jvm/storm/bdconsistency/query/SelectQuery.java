package bdconsistency.query;

import backtype.storm.tuple.Values;
import bdconsistency.trade.Trade;
import bdconsistency.ask.AsksState;
import bdconsistency.bid.BidsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 3:57 AM
 */

public class SelectQuery {

    public static class SelectStarFromAsks extends BaseQueryFunction<AsksState, Map<Long, List<Trade>>> {
        public List<Map<Long, List<Trade>>> batchRetrieve(AsksState asksState, List<TridentTuple> inputs) {

            System.out.println(MessageFormat.format("-- SelectStarFromAsks -- {0}", asksState.getTotalTrade()));

            Map<Long, List<Trade>> concurrentMap = new ConcurrentHashMap<Long, List<Trade>>();
            for (long key : asksState.getAsks().keySet()){
                Trade t = asksState.getTradeByKey(key);
                if(t == null) continue;
                if(!concurrentMap.containsKey(t.getBrokerId()))
                    concurrentMap.put(t.getBrokerId(), new ArrayList<Trade>());

                concurrentMap.get(t.getBrokerId()).add(t);
            }

            List<Map<Long, List<Trade>>> batch = new ArrayList<Map<Long, List<Trade>>>(1);
            for (TridentTuple tuple : inputs)
                batch.add(concurrentMap);
            return batch;
        }

        @Override
        public void execute(TridentTuple tuple, Map<Long, List<Trade>> result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }

    public static class SelectStarFromBids extends BaseQueryFunction<BidsState, Map<Long, List<Trade>>> {
        public List<Map<Long, List<Trade>>> batchRetrieve(BidsState bidsState, List<TridentTuple> inputs) {
            System.out.println("-- SelectStarFromBids -- " + bidsState.getTotalTrade());
            Map<Long, List<Trade>> concurrentMap = new ConcurrentHashMap<Long, List<Trade>>();
            for (long key : bidsState.getBids().keySet()){
                Trade t = bidsState.getTradeByKey(key);
                if(t == null) continue;
                if(!concurrentMap.containsKey(t.getBrokerId()))
                    concurrentMap.put(t.getBrokerId(), new ArrayList<Trade>());

                concurrentMap.get(t.getBrokerId()).add(t);
            }

            List<Map<Long, List<Trade>>> batch = new ArrayList<Map<Long, List<Trade>>>(1);
            for (TridentTuple tuple : inputs)
                batch.add(concurrentMap);
            return batch;
        }

        @Override
        public void execute(TridentTuple tuple, Map<Long, List<Trade>> result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }
}
