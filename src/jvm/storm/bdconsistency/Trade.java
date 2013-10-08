package bdconsistency;

import java.io.Serializable;
import java.util.Arrays;

/**
 * User: lbhat@damsl
 * Date: 10/2/13
 * Time: 4:58 PM
 */
public class Trade implements Serializable{

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
