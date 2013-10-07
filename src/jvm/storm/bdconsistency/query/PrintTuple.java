package bdconsistency.query;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
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
public class PrintTuple extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        for (int i = 0; i < tuple.size(); i++) {
            System.out.print(MessageFormat.format("{0}\t", tuple.getValue(i)));
        }
        System.out.println();
        collector.emit(new Values(tuple));
    }
}
