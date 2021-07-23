package flink.project;

public class OrderEvent {
    private Long orderId ;
    private String evnetType ;
    private String txId ;
    private Long eventTime ;
    public OrderEvent(){}

    public OrderEvent(Long orderId, String evnetType, String txId, Long eventTime) {
        this.orderId = orderId;
        this.evnetType = evnetType;
        this.txId = txId;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", evnetType='" + evnetType + '\'' +
                ", txId='" + txId + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OrderEvent that = (OrderEvent) o;

        if (orderId != null ? !orderId.equals(that.orderId) : that.orderId != null) return false;
        if (evnetType != null ? !evnetType.equals(that.evnetType) : that.evnetType != null) return false;
        if (txId != null ? !txId.equals(that.txId) : that.txId != null) return false;
        return eventTime != null ? eventTime.equals(that.eventTime) : that.eventTime == null;

    }

    @Override
    public int hashCode() {
        int result = orderId != null ? orderId.hashCode() : 0;
        result = 31 * result + (evnetType != null ? evnetType.hashCode() : 0);
        result = 31 * result + (txId != null ? txId.hashCode() : 0);
        result = 31 * result + (eventTime != null ? eventTime.hashCode() : 0);
        return result;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEvnetType() {
        return evnetType;
    }

    public void setEvnetType(String evnetType) {
        this.evnetType = evnetType;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
