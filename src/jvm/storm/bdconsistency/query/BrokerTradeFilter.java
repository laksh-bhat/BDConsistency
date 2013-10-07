package bdconsistency.query;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 7:03 PM
 */
public class BrokerTradeFilter  extends BaseFilter{
    public BrokerTradeFilter(long brokerId){
        this.brokerId = brokerId;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return tuple.getLong(0) == brokerId;
    }
    private final long brokerId;
}
