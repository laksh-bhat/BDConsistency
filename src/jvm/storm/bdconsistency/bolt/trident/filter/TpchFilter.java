package bdconsistency.bolt.trident.filter;

import bdconsistency.state.tpch.ITpchTable;
import bdconsistency.state.tpch.TpchState;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

import java.util.Iterator;
import java.util.Set;

/**
 * User: lbhat@damsl
 * Date: 11/4/13
 * Time: 9:49 PM
 */
public class TpchFilter {
    public static class Query3Filter extends BaseFilter {
        private int orderDateMax, shipDateMax;
        public Query3Filter (int maxOrderDate, int maxShipDate) {
            orderDateMax = maxOrderDate;
            shipDateMax = maxShipDate;
        }
        @Override
        public boolean isKeep (final TridentTuple tuple) {
            ITpchTable table = (ITpchTable) tuple.getValueByField("tpchTable");
            if (table instanceof TpchState.Orders) {
                final Set rows = table.getRows();
                Iterator<TpchState.Orders.OrderBean> iterator = rows.iterator();
                while (iterator.hasNext()) {
                    final TpchState.Orders.OrderBean bean = iterator.next();
                    if (bean.getOrderDate() > orderDateMax)
                        iterator.remove();
                }
            } else if (table instanceof TpchState.LineItem) {
                final Set rows = table.getRows();
                Iterator<TpchState.LineItem.LineItemBean> iterator = rows.iterator();
                while (iterator.hasNext()) {
                    final TpchState.LineItem.LineItemBean bean = iterator.next();
                    if (bean.getShipDate() > shipDateMax)
                        iterator.remove();
                }
            }
            return true;
        }
    }
}
