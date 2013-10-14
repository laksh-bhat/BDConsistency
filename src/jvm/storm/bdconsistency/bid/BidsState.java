package bdconsistency.bid;

import bdconsistency.trade.Trade;
import storm.trident.state.State;

import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 10/3/13
 * Time: 11:53 PM
 */
public class BidsState implements State {
    public BidsState() {
        System.out.println("Bids State Constructed");
        bids = new HashMap<Long, List<Trade>>();
    }

    public void beginCommit(Long txid) {
    }

    public void commit(Long txid) {

    }

    public void addTrade(long broker, Trade trade) {
        totalTrade++;

        if(!getBids().containsKey(broker)){
            // register this broker
            List<Trade> brokerTransactions = new ArrayList<Trade>();
            getBids().put(broker, brokerTransactions);
        }

        getBids().get(broker).add(trade);
    }

    public void removeTrade(long broker, Trade trade) {
        totalTrade++;
        // If broker isn't registered, ignore this trade
        if(!getBids().containsKey(broker))
            return;

        List<Trade> brokerTransactions = getBids().get(broker);
        for (int i = 0; i < brokerTransactions.size(); i++){
            if(brokerTransactions.get(i).getOrderId() == trade.getOrderId()){
                brokerTransactions.remove(i);
                break;
            }
        }
        if (getBids().get(broker).size() == 0)
            getBids().remove(broker);
    }

    public Map<Long, List<Trade>> getBids() {
        return bids;
    }

    public long getTotalTrade() {
        return totalTrade;
    }

    private long totalTrade;

    // Basically a multi-map
    private Map<Long, List<Trade>> bids;
}
