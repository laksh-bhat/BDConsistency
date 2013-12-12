package bdconsistency.state.bid;

import bdconsistency.trade.Trade;
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
            String tradeStr = t.getStringByField("tradeString");
            Trade trade = new Trade(tradeStr.split("\\|"));
            int operation = trade.getOperation();
<<<<<<< HEAD:src/jvm/storm/bdconsistency/state/bid/BidsUpdater.java
            long orderId =  trade.getOrderId();
            if (operation == 1) state.addTrade(orderId, trade);
            else state.removeTrade(orderId);

=======
            long brokerId =  trade.getBrokerId();
            if(operation == 1) state.addTrade(brokerId, trade);
            else state.removeTrade(brokerId, trade);
>>>>>>> master:src/jvm/storm/bdconsistency/bid/BidsUpdater.java
        }
    }
}
