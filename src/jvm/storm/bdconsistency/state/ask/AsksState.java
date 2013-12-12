package bdconsistency.state.ask;

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

    public AsksState(final long lruCacheLimit) {
        asksHashMapBackedTable = new LinkedHashMap<Long, Trade>((int)lruCacheLimit, .75F, false){
            public boolean removeEldestEntry(Map.Entry<Long, Trade> eldest) {
                return size() > lruCacheLimit;
            }
        };
        asksHashMapBackedTable = Collections.synchronizedMap(asksHashMapBackedTable);
        this.lruCacheLimit = lruCacheLimit;
    }

    public void beginCommit(Long txid) {}
    public void commit(Long txid) {}
    public long getTotalTrade() { return totalTrade; }
    public synchronized Map<Long, Trade> getAsksHashMapBackedTable() { return asksHashMapBackedTable; }

    public synchronized void addTrade(long key, Trade trade) {
        totalTrade++;
        if(!asksHashMapBackedTable.containsKey(key))
            asksHashMapBackedTable.put(key, trade);
    }

    public synchronized void removeTrade(long key) {
        totalTrade--;
        if(!asksHashMapBackedTable.containsKey(key)) return;
        if (asksHashMapBackedTable.containsKey(key)) asksHashMapBackedTable.remove(key);
    }
    public synchronized Trade getTradeByKey(long key) {
        if(asksHashMapBackedTable.containsKey(key)) return asksHashMapBackedTable.get(key);
        else return null;
    }

    public long lruCacheLimit;
    private long totalTrade;
    private Map<Long, Trade> asksHashMapBackedTable;
}
