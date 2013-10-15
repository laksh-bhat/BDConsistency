package bdconsistency.bid;

import storm.trident.state.State;
import bdconsistency.trade.Trade;

import java.io.Serializable;
import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 10/3/13
 * Time: 11:53 PM
 */

public class BidsState implements State, Serializable {
    public BidsState(final long statesize) {
        bids = new LinkedHashMap<Long, Trade>((int)statesize, .75F, false){
            // This method is called just after a new entry has been added
            public boolean removeEldestEntry(Map.Entry<Long, Trade> eldest) {
                return size() > statesize;
            }
        };
        bids = Collections.synchronizedMap(bids);
        this.stateSize = statesize;
    }

    public void beginCommit(Long txid) {}

    public void commit(Long txid) {}

    public synchronized void addTrade(long key, Trade trade) {
        totalTrade++;
        if(!bids.containsKey(key)){
            bids.put(key, trade);
        }
    }

    public synchronized void removeTrade(long key) {
        totalTrade--;
        if(!bids.containsKey(key))
            return;

        if (bids.containsKey(key))
            bids.remove(key);
    }

    public synchronized Trade getTradeByKey(long key) {
        if(bids.containsKey(key))
            return bids.get(key);
        else
            return null;
    }

    public synchronized Map<Long, Trade> getBids() {
        return bids;
    }

    public long getTotalTrade() {
        return totalTrade;
    }
    private long totalTrade;
    public long stateSize;
    private Map<Long, Trade> bids;
}
