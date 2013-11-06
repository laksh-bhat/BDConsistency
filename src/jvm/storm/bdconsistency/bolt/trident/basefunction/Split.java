package bdconsistency.bolt.trident.basefunction;

import backtype.storm.tuple.Values;
import bdconsistency.tpchschema.TpchAgenda;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * User: lbhat@damsl
 * Date: 11/4/13
 * Time: 12:08 PM
 */
public class Split {
    public static class AgendaTableSplit extends BaseFunction{

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            TpchAgenda agenda = new TpchAgenda();
            String tableName = agenda.TpchObjectConstructAndReport(tuple.getStringByField("agenda"));
            collector.emit(new Values(tableName, agenda.getOrderKey(), agenda.getCustomerKey(), agenda));
        }
    }
}
