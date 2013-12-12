package bdconsistency.state.tpch;

import bdconsistency.tpchschema.TpchAgenda;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * User: lbhat@damsl
 * Date: 11/4/13
 * Time: 8:54 PM
 */
public class TpchStateBuilder implements ReducerAggregator<ITpchTable> {

    private enum TpchTable {
        customer, lineitem, part, partsupp, supplier, region, nation, orders;
    }

    @Override
    public ITpchTable init () {
        return null;
    }

    @Override
    public ITpchTable reduce (ITpchTable currentState, final TridentTuple tuple) {
        String table = tuple.getStringByField("table");
        if (null == currentState) {
            switch (TpchTable.valueOf(table.toLowerCase())) {
                case customer:
                    currentState = new TpchState.Customer();
                    break;
                case lineitem:
                    currentState = new TpchState.LineItem();
                    break;
                case part:
                    currentState = new TpchState.Part();
                    break;
                case partsupp:
                    currentState = new TpchState.PartSupply();
                    break;
                case supplier:
                    currentState = new TpchState.Supplier();
                    break;
                case region:
                    currentState = new TpchState.Region();
                    break;
                case nation:
                    currentState = new TpchState.Nation();
                    break;
                case orders:
                    currentState = new TpchState.Orders();
                    break;
                default:
            }
        }
        currentState.add((TpchAgenda) tuple.getValueByField("agendaObject"));
        return currentState;
    }
}
