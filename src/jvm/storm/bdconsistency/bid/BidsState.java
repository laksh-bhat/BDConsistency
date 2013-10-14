package bdconsistency.bid;

import bdconsistency.trade.Trade;
import storm.trident.state.State;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * User: lbhat@damsl
 * Date: 10/3/13
 * Time: 11:53 PM
 */
public class BidsState implements State {
    public BidsState(long statesize) {
        bids = new ConcurrentHashMap<Long, List<Trade>>();
        this.stateSize = statesize;
    }

    public void beginCommit(Long txid) {
    }

    public void commit(Long txid) {

    }

    public synchronized void addTrade(long broker, Trade trade) {
        totalTrade++;

        if(!bids.containsKey(broker)){
            // register this broker
            List<Trade> brokerTransactions = new ArrayList<Trade>();
            bids.put(broker, brokerTransactions);
        }

        bids.get(broker).add(trade);
    }

    public synchronized void removeTrade(long broker, Trade trade) {
        totalTrade++;
        // If broker isn't registered, ignore this trade
        if(!bids.containsKey(broker))
            return;

        List<Trade> brokerTransactions = bids.get(broker);
        for (int i = 0; i < brokerTransactions.size(); i++){
            if(brokerTransactions.get(i).getOrderId() == trade.getOrderId()){
                brokerTransactions.remove(i);
                break;
            }
        }
        if (bids.get(broker).size() == 0)
            bids.remove(broker);
    }

    public synchronized void clearTrades(){
        this.bids.clear();
    }

    public synchronized Map<Long, List<Trade>> getBids() {
        return bids;
    }

    public long getTotalTrade() {
        return totalTrade;
    }

    private long totalTrade;

    public final long stateSize;

    // Basically a multi-map
    private Map<Long, List<Trade>> bids;
}
