package bdconsistency;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TradeConstructor {

    public static class AskTradeConstructor extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String tradeString = tuple.getString(0);
            if (tradeString.startsWith("ASKS")) {
                bdconsistency.Trade trade = new bdconsistency.Trade(tradeString.split("\\|"));
                collector.emit(new Values(trade.getBrokerId(), trade));
            }
        }
    }

    public static class BidTradeConstructor extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String tradeString = tuple.getString(0);
            if (tradeString.startsWith("BIDS")) {
                bdconsistency.Trade trade = new Trade(tradeString.split("\\|"));
                collector.emit(new Values(trade.getBrokerId(), trade));
            }
        }
    }
}