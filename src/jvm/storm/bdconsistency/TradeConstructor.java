package bdconsistency;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

public class TradeConstructor {

    public static class AskTradeConstructor extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String transactionString = tuple.getString(0);
            if (transactionString.startsWith("ASKS")) {
                bdconsistency.Trade trade = new bdconsistency.Trade(transactionString.split("|"));
                collector.emit(new Values(trade.getBrokerId(), trade));
            }
        }
    }

    public static class BidTradeConstructor extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String transactionString = tuple.getString(0);
            if (transactionString.startsWith("BIDS")) {
                bdconsistency.Trade trade = new Trade(transactionString.split("|"));
                collector.emit(new Values(trade.getBrokerId(), trade));
            }
        }
    }
}