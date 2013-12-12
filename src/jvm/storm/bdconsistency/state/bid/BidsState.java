package bdconsistency.state.bid;

import storm.trident.state.State;
import bdconsistency.trade.Trade;

import java.io.Serializable;
import java.util.*;

public class BidsState implements State, Serializable {
    public BidsState(final long lruCacheSize) {
        bidsHashMapBackedTable = new LinkedHashMap<Long, Trade>((int)lruCacheSize, .75F, false){
            public boolean removeEldestEntry(Map.Entry<Long, Trade> eldest) {
                return size() > lruCacheSize;
            }
        };
        bidsHashMapBackedTable = Collections.synchronizedMap(bidsHashMapBackedTable);
        this.lruCacheSize = lruCacheSize;
    }

    public void beginCommit(Long txid) {}

    public void commit(Long txid) {}

    public synchronized void addTrade(long key, Trade trade) {
        totalTrade++;
        if(!bidsHashMapBackedTable.containsKey(key)){
            bidsHashMapBackedTable.put(key, trade);
        }
    }

    public synchronized void removeTrade(long key) {
        totalTrade--;
        if(!bidsHashMapBackedTable.containsKey(key))
            return;

        if (bidsHashMapBackedTable.containsKey(key))
            bidsHashMapBackedTable.remove(key);
    }

    public synchronized Trade getTradeByKey(long key) {
        if(bidsHashMapBackedTable.containsKey(key)) return bidsHashMapBackedTable.get(key);
        else return null;
    }

    public synchronized Map<Long, Trade> getBidsHashMapBackedTable() {
        return bidsHashMapBackedTable;
    }

    public long getTotalTrade() {
        return totalTrade;
    }
    private long totalTrade;
    public long lruCacheSize;
    private Map<Long, Trade> bidsHashMapBackedTable;
}
