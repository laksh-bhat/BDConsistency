package bdconsistency.bid;

import bdconsistency.Trade;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 3:29 AM
 */
public class BidsUpdater extends BaseStateUpdater<BidsState> {
    public void updateState(BidsState state, List<TridentTuple> tuples, TridentCollector collector) {
        for(TridentTuple t: tuples) {
            Object trade = t.getValueByField("trade");
            if(trade instanceof Trade){
                int operation = ((Trade)trade).getOperation();
                long brokerId =  ((Trade)trade).getBrokerId();
                if(operation == 1) state.addTrade(brokerId, (Trade) trade);
                else state.removeTrade(brokerId, (Trade) trade);
            }
        }
    }
}
