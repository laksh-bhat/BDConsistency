package bdconsistency.ask;

import bdconsistency.Trade;
import bdconsistency.bid.BidsState;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 2:48 AM
 */
public class AsksUpdater extends BaseStateUpdater<BidsState> {
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