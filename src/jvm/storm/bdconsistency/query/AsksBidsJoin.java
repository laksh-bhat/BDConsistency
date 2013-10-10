package bdconsistency.query;

import backtype.storm.tuple.Values;
import bdconsistency.trade.Trade;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 10/9/13
 * Time: 6:48 PM
 */
public class AsksBidsJoin extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        System.out.println(" -- AsksBidsJoin -- ");

        Map<Long, List<Trade>> asksTable = (Map<Long, List<Trade>>) tuple.getValueByField("asks");
        Map<Long, List<Trade>> bidsTable = (Map<Long, List<Trade>>) tuple.getValueByField("bids");

        for (long broker : asksTable.keySet()) {
            long asksTotalVolume = 0, asksPrice = 0, bidsTotalVolume = 0, bidsPrice = 0;
            for (Object ask : asksTable.get(broker)) {
                asksTotalVolume += ((TridentTuple) ask).getLongByField("volume");
                asksPrice += ((TridentTuple) ask).getLongByField("price");
            }
            for (Trade bid : bidsTable.get(broker)) {
                bidsTotalVolume += bid.getVolume();
                bidsPrice += bid.getPrice();
            }

            if(Math.abs(bidsPrice - asksPrice) > 1000)
                collector.emit(new Values(broker, asksTotalVolume - bidsTotalVolume));
        }
    }
}
