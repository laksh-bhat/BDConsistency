package bdconsistency.bolt.trident.query;

import backtype.storm.tuple.Values;
import bdconsistency.utils.ObjectSizeCalculator;
import bdconsistency.state.ask.AsksState;
import bdconsistency.state.bid.BidsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 10/14/13
 * Time: 1:16 PM
 */
public class MemoryQueryDeprecated {

    public static class AsksMemoryQuery extends BaseQueryFunction<AsksState, Long> {
        @Override
        public List<Long> batchRetrieve(AsksState state, List<TridentTuple> args) {
            long memory = ObjectSizeCalculator.getObjectSize(state.getAsksHashMapBackedTable());
            List<Long> ret = new ArrayList<Long>(1);
            ret.add(memory);
            return ret;
        }
        @Override
        public void execute(TridentTuple tuple, Long result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }
    public static class BidsMemoryQuery extends BaseQueryFunction<BidsState, Long> {
        @Override
        public List<Long> batchRetrieve(BidsState state, List<TridentTuple> args) {
            long memory = ObjectSizeCalculator.getObjectSize(state.getBidsHashMapBackedTable());
            List<Long> ret = new ArrayList<Long>(1);
            ret.add(memory);
            return ret;
        }
        @Override
        public void execute(TridentTuple tuple, Long result, TridentCollector collector) {
            collector.emit(new Values(result));
        }
    }
}
