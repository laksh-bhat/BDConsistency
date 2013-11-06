package bdconsistency.tpchschema;

/**
 * User: lbhat@damsl
 * Date: 11/4/13
 * Time: 12:10 PM
 */
public class TpchAgenda {
    private int event;
    private int address;
    private int availQty;
    private int brand;
    private int clerk;
    private int comment;
    private int commitDate;
    private int container;
    private int customerKey;
    private int lineNumber;
    private int lineStatus;
    private int mfgr;
    private int marketSegment;
    private int name;
    private int nationKey;
    private int orderDate;
    private int orderKey;
    private int orderPriority;
    private int orderStatus;
    private int partKey;
    private int phone;
    private int receiptDate;
    private int regionKey;
    private int returnFlag;
    private int shipDate;
    private int shipInstruction;
    private int shipMode;
    private int shipPriority;
    private int size;
    private int supplierKey;
    private int type;
    private float accountBalance;
    private float discount;
    private float extendedPrice;
    private float quantity;
    private float retailPrice;
    private float supplyCost;
    private float tax;
    private float totalPrice;

    public String TpchObjectConstructAndReport(String agenda) {
        String[] agendaAttributes = agenda.split("\\|");
        event = Integer.valueOf(agendaAttributes[1]);
        accountBalance = Float.valueOf(agendaAttributes[2]);
        address = Integer.valueOf(agendaAttributes[3]);
        event = Integer.valueOf(agendaAttributes[4]);
        availQty = Integer.valueOf(agendaAttributes[5]);
        brand = Integer.valueOf(agendaAttributes[6]);
        clerk = Integer.valueOf(agendaAttributes[7]);
        comment = Integer.valueOf(agendaAttributes[8]);
        commitDate = Integer.valueOf(agendaAttributes[9]);
        container = Integer.valueOf(agendaAttributes[10]);
        customerKey = Integer.valueOf(agendaAttributes[11]);
        discount = Float.valueOf(agendaAttributes[12]);
        extendedPrice = Float.valueOf(agendaAttributes[13]);
        lineNumber = Integer.valueOf(agendaAttributes[14]);
        lineStatus = Integer.valueOf(agendaAttributes[15]);
        mfgr = Integer.valueOf(agendaAttributes[16]);
        marketSegment = Integer.valueOf(agendaAttributes[17]);
        name = Integer.valueOf(agendaAttributes[18]);
        nationKey = Integer.valueOf(agendaAttributes[19]);
        orderDate = Integer.valueOf(agendaAttributes[20]);
        orderKey = Integer.valueOf(agendaAttributes[21]);
        orderPriority = Integer.valueOf(agendaAttributes[22]);
        orderStatus = Integer.valueOf(agendaAttributes[23]);
        partKey = Integer.valueOf(agendaAttributes[24]);
        phone = Integer.valueOf(agendaAttributes[25]);
        quantity = Float.valueOf(agendaAttributes[26]);
        receiptDate = Integer.valueOf(agendaAttributes[27]);
        regionKey = Integer.valueOf(agendaAttributes[28]);
        retailPrice = Float.valueOf(agendaAttributes[29]);
        returnFlag = Integer.valueOf(agendaAttributes[30]);
        shipDate = Integer.valueOf(agendaAttributes[31]);
        shipInstruction = Integer.valueOf(agendaAttributes[32]);
        shipMode = Integer.valueOf(agendaAttributes[33]);
        shipPriority = Integer.valueOf(agendaAttributes[34]);
        size = Integer.valueOf(agendaAttributes[35]);
        supplierKey = Integer.valueOf(agendaAttributes[36]);
        supplyCost = Float.valueOf(agendaAttributes[37]);
        tax = Float.valueOf(agendaAttributes[38]);
        totalPrice = Float.valueOf(agendaAttributes[39]);
        type = Integer.valueOf(agendaAttributes[40]);

        return agendaAttributes[0];
    }

    public int getEvent() {
        return event;
    }

    public void setEvent(int event) {
        this.event = event;
    }

    public int getAddress() {
        return address;
    }

    public void setAddress(int address) {
        this.address = address;
    }

    public int getAvailQty() {
        return availQty;
    }

    public void setAvailQty(int availQty) {
        this.availQty = availQty;
    }

    public int getBrand() {
        return brand;
    }

    public void setBrand(int brand) {
        this.brand = brand;
    }

    public int getClerk() {
        return clerk;
    }

    public void setClerk(int clerk) {
        this.clerk = clerk;
    }

    public int getComment() {
        return comment;
    }

    public void setComment(int comment) {
        this.comment = comment;
    }

    public int getCommitDate() {
        return commitDate;
    }

    public void setCommitDate(int commitDate) {
        this.commitDate = commitDate;
    }

    public int getContainer() {
        return container;
    }

    public void setContainer(int container) {
        this.container = container;
    }

    public int getCustomerKey() {
        return customerKey;
    }

    public void setCustomerKey(int customerKey) {
        this.customerKey = customerKey;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(int lineNumber) {
        this.lineNumber = lineNumber;
    }

    public int getLineStatus() {
        return lineStatus;
    }

    public void setLineStatus(int lineStatus) {
        this.lineStatus = lineStatus;
    }

    public int getMfgr() {
        return mfgr;
    }

    public void setMfgr(int mfgr) {
        this.mfgr = mfgr;
    }

    public int getMarketSegment() {
        return marketSegment;
    }

    public void setMarketSegment(int marketSegment) {
        this.marketSegment = marketSegment;
    }

    public int getName() {
        return name;
    }

    public void setName(int name) {
        this.name = name;
    }

    public int getNationKey() {
        return nationKey;
    }

    public void setNationKey(int nationKey) {
        this.nationKey = nationKey;
    }

    public int getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(int orderDate) {
        this.orderDate = orderDate;
    }

    public int getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(int orderKey) {
        this.orderKey = orderKey;
    }

    public int getOrderPriority() {
        return orderPriority;
    }

    public void setOrderPriority(int orderPriority) {
        this.orderPriority = orderPriority;
    }

    public int getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(int orderStatus) {
        this.orderStatus = orderStatus;
    }

    public int getPartKey() {
        return partKey;
    }

    public void setPartKey(int partKey) {
        this.partKey = partKey;
    }

    public int getPhone() {
        return phone;
    }

    public void setPhone(int phone) {
        this.phone = phone;
    }

    public int getReceiptDate() {
        return receiptDate;
    }

    public void setReceiptDate(int receiptDate) {
        this.receiptDate = receiptDate;
    }

    public int getRegionKey() {
        return regionKey;
    }

    public void setRegionKey(int regionKey) {
        this.regionKey = regionKey;
    }

    public int getReturnFlag() {
        return returnFlag;
    }

    public void setReturnFlag(int returnFlag) {
        this.returnFlag = returnFlag;
    }

    public int getShipDate() {
        return shipDate;
    }

    public void setShipDate(int shipDate) {
        this.shipDate = shipDate;
    }

    public int getShipInstruction() {
        return shipInstruction;
    }

    public void setShipInstruction(int shipInstruction) {
        this.shipInstruction = shipInstruction;
    }

    public int getShipMode() {
        return shipMode;
    }

    public void setShipMode(int shipMode) {
        this.shipMode = shipMode;
    }

    public int getShipPriority() {
        return shipPriority;
    }

    public void setShipPriority(int shipPriority) {
        this.shipPriority = shipPriority;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getSupplierKey() {
        return supplierKey;
    }

    public void setSupplierKey(int supplierKey) {
        this.supplierKey = supplierKey;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public float getAccountBalance() {
        return accountBalance;
    }

    public void setAccountBalance(float accountBalance) {
        this.accountBalance = accountBalance;
    }

    public float getDiscount() {
        return discount;
    }

    public void setDiscount(float discount) {
        this.discount = discount;
    }

    public float getExtendedPrice() {
        return extendedPrice;
    }

    public void setExtendedPrice(float extendedPrice) {
        this.extendedPrice = extendedPrice;
    }

    public float getQuantity() {
        return quantity;
    }

    public void setQuantity(float quantity) {
        this.quantity = quantity;
    }

    public float getRetailPrice() {
        return retailPrice;
    }

    public void setRetailPrice(float retailPrice) {
        this.retailPrice = retailPrice;
    }

    public float getSupplyCost() {
        return supplyCost;
    }

    public void setSupplyCost(float supplyCost) {
        this.supplyCost = supplyCost;
    }

    public float getTax() {
        return tax;
    }

    public void setTax(float tax) {
        this.tax = tax;
    }

    public float getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(float totalPrice) {
        this.totalPrice = totalPrice;
    }
}
