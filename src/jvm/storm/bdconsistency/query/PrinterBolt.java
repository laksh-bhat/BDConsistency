package bdconsistency.query;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.text.MessageFormat;

/**
 * User: lbhat@damsl
 * Date: 10/7/13
 * Time: 8:24 AM
 */
public class PrinterBolt extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.out.println(tuple);
        return true;
    }

    public static class AsksPrinterBolt extends BaseFilter{

        @Override
        public boolean isKeep(TridentTuple tuple) {
            System.out.println(MessageFormat.format("ASKS -- {0}", tuple));
            return true;
        }
    }

    public static class BidsPrinterBolt extends BaseFilter{

        @Override
        public boolean isKeep(TridentTuple tuple) {
            System.out.println(MessageFormat.format("BIDS -- {0}", tuple));
            return true;
        }
    }
}
