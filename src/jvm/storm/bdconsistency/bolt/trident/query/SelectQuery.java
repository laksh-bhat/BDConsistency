package bdconsistency.bolt.trident.query;

import backtype.storm.tuple.Values;
import bdconsistency.state.ask.AsksState;
import bdconsistency.state.bid.BidsState;
import bdconsistency.trade.Trade;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * User: lbhat@damsl
 * Date: 10/29/13
 * Time: 3:37 PM
 */
public class SelectQuery {
    public static class SelectStarFromAsks extends BaseQueryFunction<AsksState, Set<Trade>> {
        public List<Set<Trade>> batchRetrieve(AsksState asksState, List<TridentTuple> inputs) {
            Set<Trade> concurrentSet = new CopyOnWriteArraySet<Trade>();
            for (long key : asksState.getAsksHashMapBackedTable().keySet())
                concurrentSet.add(asksState.getTradeByKey(key));
            List<Set<Trade>> batch = new ArrayList<Set<Trade>>(1);
            for (int i = 0; i < inputs.size(); i++)
                batch.add(concurrentSet);

            return batch;
        }

        @Override
        public void execute(TridentTuple tuple, Set<Trade> result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }

    public static class SelectStarFromBids extends BaseQueryFunction<BidsState, Set<Trade>> {
        public List<Set<Trade>> batchRetrieve(BidsState bidsState, List<TridentTuple> inputs) {
            Set<Trade> concurrentSet = new CopyOnWriteArraySet<Trade>();
            for (long key : bidsState.getBidsHashMapBackedTable().keySet())
                concurrentSet.add(bidsState.getTradeByKey(key));
            List<Set<Trade>> batch = new ArrayList<Set<Trade>>(1);
            for (int i = 0; i < inputs.size(); i++)
                batch.add(concurrentSet);

            return batch;
        }

        @Override
        public void execute(TridentTuple tuple, Set<Trade> result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }
}
