package jvm.bdconsistency;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

public class TradeConstructor extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String transactionString = tuple.getString(0);
        Trade trade = new Trade(transactionString.split("|"));
        /*collector.emit(
                new Values(trade.getTable(), trade.getOperation(), trade.getTimestamp(), trade.getOrderId(), trade.getBrokerId(),
                        trade.getPrice(), trade.getVolume()));*/
        collector.emit(new Values(trade));
    }
}