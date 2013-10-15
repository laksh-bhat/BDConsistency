package bdconsistency.ask;

import bdconsistency.trade.Trade;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;
import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 10/4/13
 * Time: 2:48 AM
 */
public class AsksUpdater extends BaseStateUpdater<AsksState> {
    public void updateState(AsksState state, List<TridentTuple> tuples, TridentCollector collector) {
        for(TridentTuple t: tuples) {
            String tradeStr = t.getStringByField("tradeString");
            Trade trade = new Trade(tradeStr.split("\\|"));
            int operation = trade.getOperation();
            long orderId =  trade.getOrderId();
            if (operation == 1) state.addTrade(orderId, trade);
            else state.removeTrade(orderId);
        }
    }
}