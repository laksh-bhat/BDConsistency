package bdconsistency.ask;

import storm.trident.state.State;
import bdconsistency.trade.Trade;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * User: lbhat@damsl
 * Date: 10/3/13
 * Time: 11:53 PM
 */
public class AsksState implements State {
    public AsksState() {
        System.out.println("Asks State Constructed");
        asks = new ConcurrentHashMap<Long, List<Trade>>();
    }

    public void beginCommit(Long txid) {}

    public void commit(Long txid) {}

    public synchronized void addTrade(long broker, Trade trade) {
        totalTrade++;
        if(!asks.containsKey(broker)){
            // register this broker
            List<Trade> brokerTransactions = new ArrayList<Trade>();
            asks.put(broker, brokerTransactions);
        }

        asks.get(broker).add(trade);
    }

    public synchronized void removeTrade(long broker, Trade trade) {
        totalTrade++;
        // If broker isn't registered, ignore this trade
        if(!asks.containsKey(broker))
            return;

        List<Trade> brokerTransactions = asks.get(broker);
        for (int i = 0; i < brokerTransactions.size(); i++){
            if(brokerTransactions.get(i).getOrderId() == trade.getOrderId()){
                brokerTransactions.remove(i);
                break;
            }
        }
        if (asks.get(broker).size() == 0)
            asks.remove(broker);
    }

    public synchronized void clearTrades() {
        this.asks.clear();
    }

    public synchronized Map<Long, List<Trade>> getAsks() {
        return asks;
    }

    public long getTotalTrade() {
        return totalTrade;
    }
    private long totalTrade;

    // Basically a multi-map
    private ConcurrentMap<Long, List<Trade>> asks;
}
