package bdconsistency.bolt.trident.query;

import backtype.storm.tuple.Values;
import bdconsistency.state.tpch.ITpchTable;
import bdconsistency.state.tpch.TpchState;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.*;


public class TpchQuery {

    public static class Query3Aggregator extends BaseAggregator<Query3Aggregator.Query3Result> {
        @Override
        public Query3Result init (final Object batchId, final TridentCollector collector) {
            return new Query3Result();
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

    public static class Query21Aggregator extends BaseAggregator<Query21Aggregator.Query21Result> {
        @Override
        public Query21Result init (final Object batchId, final TridentCollector collector) {
            return new Query21Result();
        }

        @Override
        public void aggregate (Query21Result result, final TridentTuple tuple, final TridentCollector collector) {
            if (result == null) {
                result = new Query21Result();
                result.supplierName = tuple.getIntegerByField("suppliername");
            }
            result.count++;
        }

        @Override
        public void complete (final Query21Result val, final TridentCollector collector) {
            collector.emit(new Values(val.supplierName, val.count));
        }

        class Query21Result {
            int supplierName;
            int count;
        }
    }

    public static class Query3IntermediateResult implements Serializable {
        Query3IntermediateResult (int orderKey, int orderDate, int shipPriority, double extendedPrice, double discount) {
            this.orderDate = orderDate;
            this.orderKey = orderKey;
            this.shipPriority = shipPriority;
            this.extendedPrice = extendedPrice;
            this.discount = discount;
        }

        private int orderKey, orderDate, shipPriority;
        private double extendedPrice, discount;
    }

    public static class Query3 extends BaseQueryFunction<TpchState, Set<Query3IntermediateResult>> {
        int maxOrderDate, maxShipDate, marketSegment;

        public Query3 () {}

        public Query3 (int marketSegment, int maxOrderDate, int maxShipDate) {
            this.marketSegment = marketSegment;
            this.maxOrderDate = maxOrderDate;
            this.maxShipDate = maxShipDate;
        }

        @Override
        public List<Set<Query3IntermediateResult>> batchRetrieve (final TpchState state, final List<TridentTuple> args) {

            setQueryPredicates(args);

            List<Set<Query3IntermediateResult>> returnList = new ArrayList<Set<Query3IntermediateResult>>();
            Set<Query3IntermediateResult> results = new HashSet<Query3IntermediateResult>();

            ITpchTable orders = state.getTable("orders");
            ITpchTable customer = state.getTable("customer");
            ITpchTable lineItem = state.getTable("lineitem");

            if (orders != null && customer != null && lineItem != null) {
                /*customer = filterCustomers(customer);
                orders = filterOrders(orders);
                lineItem = filterLineItems(lineItem);*/
                computeIntermediateJoinResults(results, orders, customer, lineItem);
            }
            returnList.add(results);
            return returnList;
        }

        @Override
        public void execute (final TridentTuple tuple, final Set<Query3IntermediateResult> results, final TridentCollector collector) {
            if (results != null) {
                System.err.println(MessageFormat.format("Debug: No. of Query3IntermediateResults -- size is {0}", results.size()));
                for (Query3IntermediateResult result : results)
                    collector.emit(new Values(result.orderKey, result.orderDate, result.shipPriority, result.extendedPrice, result.discount));
            }
        }

        private void setQueryPredicates (final List<TridentTuple> args) {
            // Query predicates are passed in through the drpc stream
            // I know this is horrible, but I wanted to hack something quickly
            String[] predicates = args.get(0).getStringByField("args").split(",");
            try {
                marketSegment = Integer.valueOf(predicates[0]);
                maxOrderDate = Integer.valueOf(predicates[1]);
                maxShipDate = Integer.valueOf(predicates[2]);
            } catch ( Exception ignore ) {
                // Let's not fail here;
                // We trust that the user intends to query with default values if he doesn't provide predicates/arguments
            }
        }

        private void computeIntermediateJoinResults (final Set<Query3IntermediateResult> results, final ITpchTable orders, final ITpchTable customer, final ITpchTable lineItem) {
            for (Object l : lineItem.getRows()) {
                TpchState.LineItem.LineItemBean lBean = (TpchState.LineItem.LineItemBean) l;
                for (Object o : orders.getRows()) {
                    TpchState.Orders.OrderBean orderBean = (TpchState.Orders.OrderBean) o;
                    results.add(new Query3IntermediateResult(orderBean.getOrderKey(), orderBean.getOrderDate(),
                                                             orderBean.getShipPriority(), lBean.getExtendedPrice(), lBean.getDiscount()));
                    /* for (Object c : customer.getRows()) {
                        TpchState.Customer.CustBean cBean = (TpchState.Customer.CustBean) c;
                       if (orderBean.getCustomerKey() == cBean.getCustomerKey() && lBean.getOrderKey() == orderBean.getOrderKey()) {
                        }
                    }*/
                }
            }
            System.err.println(MessageFormat.format("Debug: computeIntermediateJoinResults -- result size -- {0}", results.size()));
        }

        private ITpchTable filterCustomers (final ITpchTable customer) {
            ITpchTable filteredCustomer = new TpchState.Customer();
            final Set rows = customer.getRows();
            Iterator<TpchState.Customer.CustBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.Customer.CustBean bean = iterator.next();
                if (bean.getMarketSegment() == marketSegment)
                    filteredCustomer.add(bean);
            }
            System.err.println(MessageFormat.format("Debug: filterCustomers -- filtered {0} rows",
                                                    customer.getRows().size() - filteredCustomer.getRows().size()));

            return filteredCustomer;
        }

        private ITpchTable filterLineItems (final ITpchTable lineItem) {
            ITpchTable filteredLineItems = new TpchState.LineItem();
            final Set rows = lineItem.getRows();
            Iterator<TpchState.LineItem.LineItemBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.LineItem.LineItemBean bean = iterator.next();
                if (bean.getShipDate() > maxShipDate)
                    filteredLineItems.add(bean);
            }
            System.err.println(MessageFormat.format("Debug: filterLineItems -- filtered {0} rows",
                                                    lineItem.getRows().size() - filteredLineItems.getRows().size()));
            return filteredLineItems;
        }

        private ITpchTable filterOrders (final ITpchTable orders) {
            ITpchTable filteredOrders = new TpchState.Orders();
            final Set rows = orders.getRows();
            Iterator<TpchState.Orders.OrderBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.Orders.OrderBean bean = iterator.next();
                if (bean.getOrderDate() > maxOrderDate)
                    filteredOrders.add(bean);
            }
            System.err.println(MessageFormat.format("Debug: filterOrders -- filtered {0} rows",
                                                    orders.getRows().size() - filteredOrders.getRows().size()));
            return filteredOrders;
        }
    }


    // Query 21 Implementation
    public static class Query21 extends BaseQueryFunction<TpchState, Set<Integer>> {
        int nationName, orderStatus;

        public Query21 () {}

        @Override
        public List<Set<Integer>> batchRetrieve (final TpchState state, final List<TridentTuple> args) {

            setQueryPredicates(args);

            List<Set<Integer>> returnList = new ArrayList<Set<Integer>>();
            Set<Integer> results = new HashSet<Integer>();

            ITpchTable supplier = state.getTable("supplier");
            ITpchTable lineItem = state.getTable("lineitem");
            ITpchTable nation = state.getTable("nation");

            if (nation != null && supplier != null && lineItem != null) {
/*                lineItem = filterLineItems(lineItem);
                nation = filterNation(nation);*/
                computeIntermediateJoinResults(results, supplier, lineItem, nation);
            }
            returnList.add(results);
            return returnList;
        }

        @Override
        public void execute (final TridentTuple tuple, final Set<Integer> results, final TridentCollector collector) {
            if (results != null) {
                System.err.println(MessageFormat.format("Debug: No. of Query21IntermediateResults -- size is {0}", results.size()));
                for (Integer result : results)
                    collector.emit(new Values(result));
            }
        }

        private void setQueryPredicates (final List<TridentTuple> args) {
            // Query predicates are passed in through the drpc stream
            // I know this is horrible, but I wanted to hack something quickly
            String[] predicates = args.get(0).getStringByField("args").split(",");
            try {
                nationName = Integer.valueOf(predicates[0]);
                orderStatus = Integer.valueOf(predicates[1]);
            } catch ( Exception ignore ) {
                // Let's not fail here;
                // We trust that the user intends to query with default values if he doesn't provide predicates/arguments
            }
        }

        private void computeIntermediateJoinResults (final Set<Integer> results, final ITpchTable lineItems, final ITpchTable suppliers, final ITpchTable nations) {
            for (Object lineitem : lineItems.getRows()) {
                TpchState.LineItem.LineItemBean l = (TpchState.LineItem.LineItemBean) lineitem;
                for (Object supplier : suppliers.getRows()) {
                    TpchState.Supplier.SupplierBean s = (TpchState.Supplier.SupplierBean) supplier;
                    addToResultsAfterMatchingJoinAndAntiJoinPredicates(results, lineItems, l, s);
                }
            }
            System.err.println(MessageFormat.format("Debug: computeIntermediateJoinResults -- result size -- {0}", results.size()));
        }


        /*
            (EXISTS (SELECT * FROM Lineitem l2
            WHERE l2.orderkey = l1.orderkey
            AND l2.suppkey <> l1.suppkey))
            AND
            (NOT EXISTS
            (SELECT * FROM Lineitem l3
            WHERE l3.orderkey = l1.orderkey
            AND l3.suppkey <> l1.suppkey))
        */
        private void addToResultsAfterMatchingJoinAndAntiJoinPredicates
        (
                final Set<Integer> results,
                final ITpchTable lineItems,
                final TpchState.LineItem.LineItemBean l1,
                final TpchState.Supplier.SupplierBean s
        ) {
            if (l1.getSupplierKey() == l1.getSupplierKey()) {
            /* && s.getNationKey() == n.getNationKey() // We are already grouping by nation id*/
                boolean existsSupplierKeyNotEqual = false;
                boolean notExistsReceiptDtGtCommitDt = true;

                for (Object l2Rows : lineItems.getRows()) {
                    TpchState.LineItem.LineItemBean l2 = (TpchState.LineItem.LineItemBean) l2Rows;
                    if (l2.getSupplierKey() != l1.getSupplierKey()) {
                        existsSupplierKeyNotEqual = true;
                        if (l2.getReceiptDate() > l2.getCommitDate()) {
                            notExistsReceiptDtGtCommitDt = false;
                            break;
                        }
                    }
                }
                if (existsSupplierKeyNotEqual && notExistsReceiptDtGtCommitDt) {
                    results.add(s.getName());
                }
            }
        }

        private ITpchTable filterLineItems (final ITpchTable lineItem) {
            ITpchTable filteredLineItems = new TpchState.LineItem();
            final Set rows = lineItem.getRows();
            Iterator<TpchState.LineItem.LineItemBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.LineItem.LineItemBean bean = iterator.next();
                if (bean.getReceiptDate() > bean.getCommitDate())
                    filteredLineItems.add(bean);
            }
            System.err.println(MessageFormat.format("Debug: filterLineItems -- filtered {0} rows",
                                                    lineItem.getRows().size() - filteredLineItems.getRows().size()));
            return filteredLineItems;
        }


        private ITpchTable filterNation (final ITpchTable nation) {
            ITpchTable filteredNation = new TpchState.Nation();
            final Set rows = nation.getRows();
            Iterator<TpchState.Nation.NationBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.Nation.NationBean bean = iterator.next();
                if (bean.getName() == nationName)
                    filteredNation.add(bean);
            }
            System.err.println(MessageFormat.format("Debug: filterNation -- filtered {0} rows",
                                                    nation.getRows().size() - filteredNation.getRows().size()));
            return filteredNation;
        }

        private ITpchTable filterOrders (final ITpchTable orders) {
            ITpchTable filteredOrders = new TpchState.Orders();
            final Set rows = orders.getRows();
            Iterator<TpchState.Orders.OrderBean> iterator = rows.iterator();
            while (iterator.hasNext()) {
                final TpchState.Orders.OrderBean bean = iterator.next();
                if (bean.getOrderStatus() == orderStatus)
                    filteredOrders.add(bean);
            }
            System.err.println(MessageFormat.format("Debug: filterOrders -- filtered {0} rows",
                                                    orders.getRows().size() - filteredOrders.getRows().size()));
            return filteredOrders;
        }
    }
}
