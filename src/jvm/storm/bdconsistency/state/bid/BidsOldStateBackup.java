package bdconsistency.state.bid;

import bdconsistency.trade.Trade;
import storm.trident.state.State;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * User: lbhat@damsl
 * Date: 10/14/13
 * Time: 10:54 PM
 */

/**
 * User: lbhat@damsl
 * Date: 10/3/13
 * Time: 11:53 PM
 */
public class BidsOldStateBackup implements State, Serializable {
    public BidsOldStateBackup(final long statesize) {
        bids = new LinkedHashMap<Long, List<Trade>>((int) 100, .75F, false){
            // This method is called just after a new entry has been added
            public boolean removeEldestEntry(Map.Entry eldest) {
                return size() > statesize;
            }
        };
        this.stateSize = statesize;
    }

    public void beginCommit(Long txid) {
    }

    public void commit(Long txid) {

    }

    public synchronized void addTrade(long broker, Trade trade) {
        totalTrade++;

        if (!bids.containsKey(broker)) {
            // register this broker
            List<Trade> brokerTransactions = new CopyOnWriteArrayList<Trade>();
            bids.put(broker, brokerTransactions);
        }

        bids.get(broker).add(trade);
    }

    public synchronized void removeTrade(long broker, Trade trade) {
        totalTrade--;
        // If broker isn't registered, ignore this trade
        if (!bids.containsKey(broker))
            return;

        List<Trade> brokerTransactions = bids.get(broker);
        for (int i = 0; i < brokerTransactions.size(); i++) {
            if (brokerTransactions.get(i).getOrderId() == trade.getOrderId()) {
                brokerTransactions.remove(i);
                break;
            }
        }
        if (bids.get(broker).size() == 0)
            bids.remove(broker);
    }

    public synchronized void clearTrades() {
        this.bids.clear();
    }

    public synchronized List<Trade> getTradeByBroker(long broker) {
        return bids.get(broker);
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
