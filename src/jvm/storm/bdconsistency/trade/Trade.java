package bdconsistency.trade;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

/**
 * User: lbhat@damsl
 * Date: 10/2/13
 * Time: 4:58 PM
 */
public class Trade implements Serializable{


    public static class TradeConstructor {
        public static class AskTradeConstructor extends BaseFunction {
            @Override
            public void execute(TridentTuple tuple, TridentCollector collector) {
                String tradeString = tuple.getString(0);
                if (tradeString.startsWith("ASKS")) {
                    Trade trade = new Trade(tradeString.split("\\|"));
                    collector.emit(new Values(trade.getBrokerId(), trade));
                }
            }
        }

        public static class BidTradeConstructor extends BaseFunction {
            @Override
            public void execute(TridentTuple tuple, TridentCollector collector) {
                String tradeString = tuple.getString(0);
                if (tradeString.startsWith("BIDS")) {
                    Trade trade = new Trade(tradeString.split("\\|"));
                    collector.emit(new Values(trade.getBrokerId(), trade));
                }
            }
        }
    }


    public Trade(String[] message) {
        this.table      = message[0];
        this.operation  = Integer.valueOf(message[1]);
        this.timestamp  = Long.valueOf(message[2]);
        this.orderId    = Integer.valueOf(message[3]);
        this.brokerId   = Integer.valueOf(message[4]);
        this.price      = Double.valueOf(message[5]);
        this.volume     = Double.valueOf(message[6]);
    }

    public Long getTimestamp() {
        return timestamp;
    }
    public int getOrderId() {
        return orderId;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public double getVolume() {
        return volume;
    }

    public double getPrice() {
        return price;
    }

    public String getTable() {
        return table;
    }

    public int getOperation() {
        return operation;
    }

    private Long timestamp;
    private double volume;

    private double price;
    private int orderId;
    private int brokerId;

    private int operation;

    private String table;
}
