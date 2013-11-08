package bdconsistency.state.tpch;

import backtype.storm.task.IMetricsContext;
import bdconsistency.tpchschema.TpchAgenda;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;

import java.io.Serializable;
import java.util.*;


public class TpchState implements State, IBackingMap<ITpchTable>, Serializable {
    private enum TpchTableEnum {
        customer, lineitem, part, partsupp, supplier, region, nation, orders
    }

    private Map<String, ITpchTable> tables;

    public TpchState () {tables = new HashMap<String, ITpchTable>();}

    public ITpchTable getTable(String table){return tables.get(table);}

    public void updateTable (String tableName, TpchAgenda agenda) {
        if (tables.containsKey(tableName)) {
            tables.get(tableName).add(agenda);
        } else {
            ITpchTable tpchTable = null;
            switch (TpchTableEnum.valueOf(tableName.toLowerCase())) {
                case customer:
                    tpchTable = new Customer();
                    break;
                case lineitem:
                    tpchTable = new LineItem();
                    break;
                case part:
                    tpchTable = new Part();
                    break;
                case partsupp:
                    tpchTable = new PartSupply();
                    break;
                case supplier:
                    tpchTable = new Supplier();
                    break;
                case region:
                    tpchTable = new Region();
                    break;
                case nation:
                    tpchTable = new Nation();
                    break;
                case orders:
                    tpchTable = new Orders();
                    break;
                default:
            }
            tables.put(tableName, tpchTable);
        }
    }

    @Override
    public List<ITpchTable> multiGet (final List<List<Object>> keys) {
        List<ITpchTable> tables = new LinkedList();
        for (List<Object> key : keys) {
            String tableName = ((String) key.get(0)).toLowerCase();
            tables.add(this.tables.get(tableName));
        }
        return tables;
    }

    @Override
    public void multiPut (final List<List<Object>> keys, final List<ITpchTable> tableList) {
        for (int i = 0; i < keys.size(); i++) {
            final List<Object> key = keys.get(i);
            String tableName = ((String) key.get(0)).toLowerCase();
            if (!this.tables.containsKey(tableName)) {
                this.tables.put(tableName, tableList.get(i));
            } else {
                ITpchTable table = this.tables.get(tableName);
                table.append(tableList.get(i));
            }
        }
    }

    @Override
    public void beginCommit (final Long txid) {}

    @Override
    public void commit (final Long txid) {}

    public static class Customer implements ITpchTable {

        @Override
        public String getTableName () {
            return this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public Set getRows () {
            return this.customerInfo;
        }

        @Override
        public void append (final ITpchTable table) {
            for (Object c : table.getRows())
                if (c instanceof CustBean)
                    this.customerInfo.add((CustBean) c);
        }

        @Override
        public void beginCommit (final Long txid) {}

        @Override
        public void commit (final Long txid) {}

        public class CustBean {

            private int   customerKey;
            private int   name;
            private int   address;
            private int   nationKey;
            private int   phone;
            private int   marketSegment;
            private int   comment;
            private float accountBalance;

            public int getCustomerKey () {
                return customerKey;
            }

            public int getName () {
                return name;
            }

            public int getAddress () {
                return address;
            }

            public int getNationKey () {
                return nationKey;
            }

            public int getPhone () {
                return phone;
            }

            public int getMarketSegment () {
                return marketSegment;
            }

            public int getComment () {
                return comment;
            }

            public float getAccountBalance () {
                return accountBalance;
            }

        }

        private Set<CustBean> customerInfo = new HashSet<CustBean>();

        public void add (TpchAgenda agenda) {
            CustBean bean = new CustBean();
            bean.accountBalance = agenda.getAccountBalance();
            bean.address = agenda.getAddress();
            bean.comment = agenda.getComment();
            bean.customerKey = agenda.getCustomerKey();
            bean.name = agenda.getName();
            bean.nationKey = agenda.getNationKey();
            bean.marketSegment = agenda.getMarketSegment();
            bean.phone = agenda.getPhone();
            customerInfo.add(bean);
        }

    }

    public static class LineItem implements ITpchTable {

        @Override
        public void beginCommit (final Long txid) {}

        @Override
        public void commit (final Long txid) {}

        @Override
        public String getTableName () {
            return this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public Set getRows () {
            return this.lineItem;
        }

        @Override
        public void append (final ITpchTable table) {
            for (Object o : table.getRows())
                if (o instanceof LineItemBean)
                    this.lineItem.add((LineItemBean) o);
        }

        public class LineItemBean {

            private int   orderKey;
            private int   partKey;
            private int   supplierKey;
            private int   lineNumber;
            private int   returnFlag;
            private int   lineStatus;
            private int   shipDate;
            private int   commitDate;
            private int   receiptDate;
            private int   shipInstruction;
            private int   shipMode;
            private int   comment;
            private float quantity;
            private float extendedPrice;
            private float discount;
            private float tax;

            public int getOrderKey () {
                return orderKey;
            }

            public int getPartKey () {
                return partKey;
            }

            public int getSupplierKey () {
                return supplierKey;
            }

            public int getLineNumber () {
                return lineNumber;
            }

            public int getReturnFlag () {
                return returnFlag;
            }

            public int getLineStatus () {
                return lineStatus;
            }

            public int getShipDate () {
                return shipDate;
            }

            public int getCommitDate () {
                return commitDate;
            }

            public int getReceiptDate () {
                return receiptDate;
            }

            public int getShipInstruction () {
                return shipInstruction;
            }

            public int getShipMode () {
                return shipMode;
            }

            public int getComment () {
                return comment;
            }

            public float getQuantity () {
                return quantity;
            }

            public float getExtendedPrice () {
                return extendedPrice;
            }

            public float getDiscount () {
                return discount;
            }

            public float getTax () {
                return tax;
            }

        }

        private Set<LineItemBean> lineItem = new HashSet<LineItemBean>();

        public void add (TpchAgenda agenda) {
            LineItemBean bean = new LineItemBean();
            bean.comment = agenda.getComment();
            bean.commitDate = agenda.getCommitDate();
            bean.discount = agenda.getDiscount();
            bean.extendedPrice = agenda.getExtendedPrice();
            bean.tax = agenda.getTax();
            bean.supplierKey = agenda.getSupplierKey();
            bean.shipMode = agenda.getShipMode();
            bean.shipDate = agenda.getShipDate();
            bean.shipInstruction = agenda.getShipInstruction();
            bean.returnFlag = agenda.getReturnFlag();
            bean.receiptDate = agenda.getReceiptDate();
            bean.quantity = agenda.getQuantity();
            bean.partKey = agenda.getPartKey();
            bean.orderKey = agenda.getOrderKey();
            bean.lineNumber = agenda.getLineNumber();
            bean.lineStatus = agenda.getLineStatus();
            lineItem.add(bean);
        }

    }

    public static class Nation implements ITpchTable {

        @Override
        public void beginCommit (final Long txid) {}

        @Override
        public void commit (final Long txid) {}

        @Override
        public String getTableName () {
            return this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public Set getRows () {
            return this.nation;
        }

        @Override
        public void append (final ITpchTable table) {
            for (Object o : table.getRows())
                if (o instanceof NationBean)
                    this.nation.add((NationBean) o);
        }

        public class NationBean {

            private int nationKey;
            private int name;
            private int regionKey;
            private int comment;

            public int getNationKey () {
                return nationKey;
            }

            public int getName () {
                return name;
            }

            public int getRegionKey () {
                return regionKey;
            }

            public int getComment () {
                return comment;
            }

        }

        private Set<NationBean> nation = new HashSet<NationBean>();

        public void add (TpchAgenda agenda) {
            NationBean bean = new NationBean();
            bean.comment = agenda.getComment();
            bean.name = agenda.getName();
            bean.nationKey = agenda.getNationKey();
            bean.regionKey = agenda.getRegionKey();
            nation.add(bean);
        }

    }

    public static class Orders implements ITpchTable {

        @Override
        public void beginCommit (final Long txid) {}

        @Override
        public void commit (final Long txid) {}

        @Override
        public String getTableName () {
            return this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public Set getRows () {
            return this.orders;
        }

        @Override
        public void append (final ITpchTable table) {
            for (Object o : table.getRows())
                if (o instanceof OrderBean)
                    this.orders.add((OrderBean) o);
        }

        public class OrderBean {

            private int   orderKey;
            private int   customerKey;
            private int   orderStatus;
            private int   orderDate;
            private int   orderPriority;
            private int   clerk;
            private int   shipPriority;
            private int   comment;
            private float totalPrice;

            public int getOrderKey () {
                return orderKey;
            }

            public int getCustomerKey () {
                return customerKey;
            }

            public int getOrderStatus () {
                return orderStatus;
            }

            public int getOrderDate () {
                return orderDate;
            }

            public int getOrderPriority () {
                return orderPriority;
            }

            public int getClerk () {
                return clerk;
            }

            public int getShipPriority () {
                return shipPriority;
            }

            public int getComment () {
                return comment;
            }

            public float getTotalPrice () {
                return totalPrice;
            }

        }

        private Set<OrderBean> orders = new HashSet<OrderBean>();

        public void add (TpchAgenda agenda) {
            OrderBean bean = new OrderBean();
            bean.clerk = agenda.getClerk();
            bean.comment = agenda.getComment();
            bean.customerKey = agenda.getCustomerKey();
            bean.totalPrice = agenda.getTotalPrice();
            bean.shipPriority = agenda.getShipPriority();
            bean.orderDate = agenda.getOrderDate();
            bean.orderKey = agenda.getOrderKey();
            bean.orderPriority = agenda.getOrderPriority();
            bean.orderStatus = agenda.getOrderStatus();
            orders.add(bean);
        }

    }

    public static class PartSupply implements ITpchTable {

        @Override
        public void beginCommit (final Long txid) {}

        @Override
        public void commit (final Long txid) {}

        @Override
        public String getTableName () {
            return this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public Set getRows () {
            return this.partsSupply;
        }

        @Override
        public void append (final ITpchTable table) {
            for (Object o : table.getRows())
                if (o instanceof PartSupplyBean)
                    this.partsSupply.add((PartSupplyBean) o);
        }

        public class PartSupplyBean {

            private int   partKey;
            private int   supplyKey;
            private int   availableQuantity;
            private int   comment;
            private float supplyCost;

            public int getPartKey () {
                return partKey;
            }

            public int getSupplyKey () {
                return supplyKey;
            }

            public int getAvailableQuantity () {
                return availableQuantity;
            }

            public int getComment () {
                return comment;
            }

            public float getSupplyCost () {
                return supplyCost;
            }

        }

        private Set<PartSupplyBean> partsSupply = new HashSet<PartSupplyBean>();

        public void add (TpchAgenda agenda) {
            PartSupplyBean bean = new PartSupplyBean();
            bean.availableQuantity = agenda.getAvailQty();
            bean.comment = agenda.getComment();
            bean.partKey = agenda.getPartKey();
            bean.supplyCost = agenda.getSupplyCost();
            bean.supplyKey = agenda.getSupplierKey();
            partsSupply.add(bean);
        }

    }

    public static class Part implements ITpchTable {

        @Override
        public void beginCommit (final Long txid) {}

        @Override
        public void commit (final Long txid) {}

        @Override
        public String getTableName () {
            return this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public Set getRows () {
            return this.part;
        }

        @Override
        public void append (final ITpchTable table) {
            for (Object o : table.getRows())
                if (o instanceof PartBean)
                    this.part.add((PartBean) o);
        }

        public class PartBean {

            private int   partKey;
            private int   name;
            private int   mfgr;
            private int   brand;
            private int   type;
            private int   size;
            private int   container;
            private int   comment;
            private float retailPrice;

            public int getPartKey () {
                return partKey;
            }

            public int getName () {
                return name;
            }

            public int getMfgr () {
                return mfgr;
            }

            public int getBrand () {
                return brand;
            }

            public int getType () {
                return type;
            }

            public int getSize () {
                return size;
            }

            public int getContainer () {
                return container;
            }

            public int getComment () {
                return comment;
            }

            public float getRetailPrice () {
                return retailPrice;
            }

        }

        private Set<PartBean> part = new HashSet<PartBean>();

        public void add (TpchAgenda agenda) {
            PartBean bean = new PartBean();
            bean.brand = agenda.getBrand();
            bean.comment = agenda.getComment();
            bean.container = agenda.getContainer();
            bean.type = agenda.getType();
            bean.size = agenda.getSize();
            bean.retailPrice = agenda.getRetailPrice();
            bean.partKey = agenda.getPartKey();
            bean.name = agenda.getName();
            bean.mfgr = agenda.getMfgr();
            part.add(bean);
        }

    }

    public static class Region implements ITpchTable {

        @Override
        public void beginCommit (final Long txid) {}

        @Override
        public void commit (final Long txid) {}

        @Override
        public String getTableName () {
            return this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public Set getRows () {
            return this.region;
        }

        @Override
        public void append (final ITpchTable table) {
            for (Object o : table.getRows())
                if (o instanceof RegionBean)
                    this.region.add((RegionBean) o);
        }

        public class RegionBean {

            private int regionKey;
            private int name;
            private int comment;

            public int getRegionKey () {
                return regionKey;
            }

            public int getName () {
                return name;
            }

            public int getComment () {
                return comment;
            }

        }

        private Set<RegionBean> region = new HashSet<RegionBean>();

        public void add (TpchAgenda agenda) {
            RegionBean bean = new RegionBean();
            bean.comment = agenda.getComment();
            bean.name = agenda.getName();
            bean.regionKey = agenda.getRegionKey();
            region.add(bean);
        }

    }

    public static class Supplier implements ITpchTable {

        @Override
        public void beginCommit (final Long txid) {}

        @Override
        public void commit (final Long txid) {}

        @Override
        public String getTableName () {
            return this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public Set getRows () {
            return this.supplier;
        }

        @Override
        public void append (final ITpchTable table) {
            for (Object o : table.getRows())
                if (o instanceof SupplierBean)
                    this.supplier.add((SupplierBean) o);
        }

        public class SupplierBean {

            private int   supplierKey;
            private int   name;
            private int   address;
            private int   nationKey;
            private int   phone;
            private int   comment;
            private float accountBalance;

            public int getSupplierKey () {
                return supplierKey;
            }

            public int getName () {
                return name;
            }

            public int getAddress () {
                return address;
            }

            public int getNationKey () {
                return nationKey;
            }

            public int getPhone () {
                return phone;
            }

            public int getComment () {
                return comment;
            }

            public float getAccountBalance () {
                return accountBalance;
            }

        }

        private Set<SupplierBean> supplier = new HashSet<SupplierBean>();

        public void add (TpchAgenda agenda) {
            SupplierBean bean = new SupplierBean();
            bean.accountBalance = agenda.getAccountBalance();
            bean.address = agenda.getAddress();
            bean.comment = agenda.getComment();
            bean.name = agenda.getName();
            bean.supplierKey = agenda.getSupplierKey();
            bean.phone = agenda.getPhone();
            bean.nationKey = agenda.getNationKey();
            supplier.add(bean);
        }

    }

    public static StateFactory FACTORY = new StateFactory() {
        public State makeState (Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            // our logic is fully idempotent => no Opaque Map or Transactional Map required here
            //return NonTransactionalMap.build(new TpchState());
            return new TpchState();
        }
    };
}

