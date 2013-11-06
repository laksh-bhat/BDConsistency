package bdconsistency.bolt.trident.query;

import backtype.storm.tuple.Values;
import bdconsistency.trade.Trade;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Iterator;
import java.util.Set;

/*
* SELECT SUM(a.price - b.price)
* FROM Bids b, Asks a
* WHERE b.volume>0.0001*(SELECT SUM(b1.volume) FROM Bids b1)
* AND a.volume>0.0001*(SELECT SUM(a1.volume) FROM Asks a1);
* */
public class PspQuery extends BaseFunction {
    @Override
    public final void execute(TridentTuple tuple, TridentCollector collector) {
        Set<Trade> asks      = (Set<Trade>) tuple.getValueByField("asks");
        Set<Trade> bids      = (Set<Trade>) tuple.getValueByField("bids");
        double bidsVolumeRhs = 0.0001 * getTotalVolume(bids);
        double asksVolumeRhs = 0.0001 * getTotalVolume(asks);

        filterTradesBasedOnVolume(asks, bids, bidsVolumeRhs, asksVolumeRhs);
        collector.emit(new Values(getSpread(asks, bids)));
    }

    private double getSpread(Set<Trade> asks, Set<Trade> bids) {
        double spread = 0;
        for (Trade a : asks)
            for (Trade b : bids)
                spread += a.getPrice() - b.getPrice();
        return spread;
    }

    private void filterTradesBasedOnVolume(Set<Trade> asks, Set<Trade> bids, double bidsVolumeRhs, double asksVolumeRhs) {
        Iterator<Trade> bidsIterator = bids.iterator();
        while (bidsIterator.hasNext()){
            Trade t = bidsIterator.next();
            if (t.getVolume() <= bidsVolumeRhs)
                bidsIterator.remove();
        }
        Iterator<Trade> asksIterator = asks.iterator();
        while (asksIterator.hasNext()){
            Trade t = asksIterator.next();
            if (t.getVolume() <= asksVolumeRhs)
                asksIterator.remove();
        }
    }

    private long getTotalVolume(Set<Trade> trades) {
        long totalVolume = 0;
        for (Trade trade : trades)
            totalVolume += trade.getVolume();
        return totalVolume;
    }
}
