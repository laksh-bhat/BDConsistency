package bdconsistency.bolt.trident.filter;

import bdconsistency.tpchschema.TpchAgenda;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * User: lbhat@damsl
 * Date: 11/4/13
 * Time: 9:49 PM
 */
public class TpchFilter {
    public static class Query3Filter extends BaseFilter {
        private Long orderDateKeepAfter, shipDateKeepAfter, marketSegment;

        public Query3Filter (Long marketSegment, Long maxOrderDate, Long maxShipDate) {
            this.orderDateKeepAfter = maxOrderDate;
            this.shipDateKeepAfter = maxShipDate;
            this.marketSegment = marketSegment;
        }

        @Override
        public boolean isKeep (final TridentTuple tuple) {
            TpchAgenda agenda = (TpchAgenda) tuple.getValueByField("agendaObject");
            String table = tuple.getStringByField("table");
            if (table.equalsIgnoreCase("orders") && agenda.getOrderDate() > orderDateKeepAfter)
                return true;
            else if (table.equalsIgnoreCase("lineitem") && agenda.getShipDate() > shipDateKeepAfter)
                return true;
            else if (table.equalsIgnoreCase("customer") && agenda.getMarketSegment() == marketSegment)
                return true;
            else return true;
        }
    }
}
