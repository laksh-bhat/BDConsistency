package bdconsistency.ask;

import bdconsistency.Trade;
import storm.trident.state.State;

import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 10/3/13
 * Time: 11:53 PM
 */
public class AsksState implements State {
    public AsksState() {
        asks = new HashMap<Long, List<Trade>>();
    }

    public void beginCommit(Long txid) {
    }

    public void commit(Long txid) {

    }

    public void addTrade(long broker, Trade trade) {
        if(!getAsks().containsKey(broker)){
            // register this broker
            List<Trade> brokerTransactions = new ArrayList<Trade>();
            getAsks().put(broker, brokerTransactions);
        }

        getAsks().get(broker).add(trade);
    }

    public void removeTrade(long broker, Trade trade) {
        // If broker isn't registered, ignore this trade
        if(!getAsks().containsKey(broker))
            return;

        List<Trade> brokerTransactions = getAsks().get(broker);
        for (int i = 0; i < brokerTransactions.size(); i++){
            if(brokerTransactions.get(i).getOrderId() == trade.getOrderId()){
                brokerTransactions.remove(i);
                break;
            }
        }
        if (getAsks().get(broker).size() == 0)
            getAsks().remove(broker);
    }

    public Map<Long, List<Trade>> getAsks() {
        return asks;
    }

    // Basically a multi-map
    private Map<Long, List<Trade>> asks;
}
