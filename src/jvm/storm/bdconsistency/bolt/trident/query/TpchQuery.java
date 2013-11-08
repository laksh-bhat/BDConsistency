package bdconsistency.bolt.trident.query;

import backtype.storm.tuple.Values;
import bdconsistency.state.tpch.ITpchTable;
import bdconsistency.state.tpch.TpchState;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.*;


public class TpchQuery {

    public static class Query3Aggregator extends BaseAggregator<Query3Aggregator.Query3Result> {
        @Override
        public Query3Result init (final Object batchId, final TridentCollector collector) {
            return null;
        }

        @Override
        public void aggregate (Query3Result query3Result, final TridentTuple tuple, final TridentCollector collector) {
            if (query3Result == null)
                query3Result = new Query3Result();
            query3Result.query3 += tuple.getDoubleByField("extendedprice") * (1 - tuple.getDoubleByField("discount"));
        }

        @Override
        public void complete (final Query3Result val, final TridentCollector collector) {
            collector.emit(new Values(val.query3));
        }

        class Query3Result {
            double query3;
        }
    }

    /*
     SELECT o.orderkey, o.orderdate, o.shippriority, SUM(extendedprice * (1 - discount)) AS query3
     FROM Customer c, Orders o, Lineitem l
     WHERE c.mktsegment = 'BUILDING'
     AND o.custkey = c.custkey
     AND l.orderkey = o.orderkey
                                     AND o.orderdate < DATE('1995-03-15')
                                     AND l.shipdate > DATE('1995-03-15')
     GROUP BY o.orderkey, o.orderdate, o.shippriority;
     **/
    public static class Query3IntermediateResult implements Serializable {
        Query3IntermediateResult (int orderKey, int orderDate, int shipPriority, double extendedPrice, double discount) {
            this.orderDate = orderDate;
            this.orderKey = orderKey;
            this.shipPriority = shipPriority;
            this.extendedPrice = extendedPrice;
            this.discount = discount;
        }

        int orderKey, orderDate, shipPriority;
        double extendedPrice, discount;
    }

    public static class Query3 extends BaseQueryFunction<TpchState, List<Query3IntermediateResult>> {
        int maxOrderDate, maxShipDate, marketSegment;

        public Query3 () {}

        public Query3 (int marketSegment, int maxOrderDate, int maxShipDate) {
            this.marketSegment = marketSegment;
            this.maxOrderDate = maxOrderDate;
            this.maxShipDate = maxShipDate;
        }

        @Override
        public List<List<Query3IntermediateResult>> batchRetrieve (final TpchState state, final List<TridentTuple> args) {
            // Query predicates are passed in through the drpc stream
            // I know this is horrible, but I wanted to hack something quickly
            String[] predicates = args.get(0).getStringByField("args").split(",");
            try {
                marketSegment = Integer.valueOf(predicates[0]);
                maxOrderDate  = Integer.valueOf(predicates[1]);
                maxShipDate   = Integer.valueOf(predicates[2]);
            } catch ( NumberFormatException ignore ) {}

            List<List<Query3IntermediateResult>> returnList = new LinkedList<List<Query3IntermediateResult>>();
            List<Query3IntermediateResult> results = new ArrayList<Query3IntermediateResult>();
            ITpchTable orders = state.getTable("orders");
            ITpchTable customer = state.getTable("customer");
            ITpchTable lineItem = state.getTable("lineitem");

            if (orders != null && customer != null && lineItem != null) {
                //filterCustomers(customer);
                filterOrders(orders);
                filterLineItems(lineItem);
                computeIntermediateJoinResults(results, orders, customer, lineItem);
            }
            returnList.add(results);


            // The return list must have the same number of elements as that of the keys.
            // These "null" will be ignored while emitting tuples.
            for (int i = returnList.size() ; i < args.size(); i++) returnList.add(null);
            return returnList;
        }

        @Override
        public void execute (final TridentTuple tuple, final List<Query3IntermediateResult> results, final TridentCollector collector) {
            if (results != null)
                for (Query3IntermediateResult result : results)
                    collector.emit(new Values(result.orderKey, result.orderDate, result.shipPriority, result.extendedPrice));
        }

        private void computeIntermediateJoinResults (final List<Query3IntermediateResult> results, final ITpchTable orders, final ITpchTable customer, final ITpchTable lineItem) {
            for (Object l : lineItem.getRows()) {
                TpchState.LineItem.LineItemBean lBean = (TpchState.LineItem.LineItemBean) l;
                for (Object o : orders.getRows()) {
                    TpchState.Orders.OrderBean orderBean = (TpchState.Orders.OrderBean) o;
                    for (Object c : customer.getRows()) {
                        TpchState.Customer.CustBean cBean = (TpchState.Customer.CustBean) c;
                        if (orderBean.getCustomerKey() == cBean.getCustomerKey() && lBean.getOrderKey() == orderBean.getOrderKey()) {
                            results.add(new Query3IntermediateResult(orderBean.getOrderKey(), orderBean.getOrderDate(),
                                                                     orderBean.getShipPriority(), lBean.getExtendedPrice(), lBean.getDiscount()));
                        }
                    }
                }
            }
        }

        private void filterCustomers (final ITpchTable customer) {
            final Set rows = customer.getRows();
            Iterator<TpchState.Customer.CustBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.Customer.CustBean bean = iterator.next();
                if (bean.getMarketSegment() != marketSegment)
                    iterator.remove();
            }
        }

        private void filterLineItems (final ITpchTable lineItem) {
            final Set rows = lineItem.getRows();
            Iterator<TpchState.LineItem.LineItemBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.LineItem.LineItemBean bean = iterator.next();
                if (bean.getShipDate() > maxShipDate)
                    iterator.remove();
            }
        }

        private void filterOrders (final ITpchTable orders) {
            final Set rows = orders.getRows();
            Iterator<TpchState.Orders.OrderBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.Orders.OrderBean bean = iterator.next();
                if (bean.getOrderDate() > maxOrderDate)
                    iterator.remove();
            }
        }
    }
}
