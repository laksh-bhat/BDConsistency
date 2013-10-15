package bdconsistency.ask;

import storm.trident.state.State;
import bdconsistency.trade.Trade;

import java.io.Serializable;
import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 10/3/13
 * Time: 11:53 PM
 */
public class AsksState implements State, Serializable {
    public AsksState(final long statesize) {
        asks = new LinkedHashMap<Long, Trade>(100, .75F, false){
            // This method is called just after a new entry has been added
            public boolean removeEldestEntry(Map.Entry<Long, Trade> eldest) {
                return size() > statesize;
            }
        };
        asks = Collections.synchronizedMap(asks);
        this.stateSize = statesize;
    }

    public void beginCommit(Long txid) {}

    public void commit(Long txid) {}

    public synchronized void addTrade(long key, Trade trade) {
        totalTrade++;
        if(!asks.containsKey(key)){
            asks.put(key, trade);
        }
    }

    public synchronized void removeTrade(long key) {
        totalTrade--;
        if(!asks.containsKey(key))
            return;

        if (asks.containsKey(key))
            asks.remove(key);
    }

    public synchronized Trade getTradeByKey(long key) {
        if(asks.containsKey(key))
            return asks.get(key);
        else
            return null;
    }

    public synchronized Map<Long, Trade> getAsks() {
        return asks;
    }

    public long getTotalTrade() {
        return totalTrade;
    }
    private long totalTrade;
    public long stateSize;
    // Basically a multi-map
    private Map<Long, Trade> asks;
}
