package flink.project;

public class TxEvent {
    private String txId ;
    private String payChannel ;
    private Long eventTime ;

    public TxEvent(){}

    public TxEvent(String txId, String payChannel, Long eventTime) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "TxEvent{" +
                "txId='" + txId + '\'' +
                ", payChannel='" + payChannel + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TxEvent txEvent = (TxEvent) o;

        if (txId != null ? !txId.equals(txEvent.txId) : txEvent.txId != null) return false;
        if (payChannel != null ? !payChannel.equals(txEvent.payChannel) : txEvent.payChannel != null) return false;
        return eventTime != null ? eventTime.equals(txEvent.eventTime) : txEvent.eventTime == null;

    }

    @Override
    public int hashCode() {
        int result = txId != null ? txId.hashCode() : 0;
        result = 31 * result + (payChannel != null ? payChannel.hashCode() : 0);
        result = 31 * result + (eventTime != null ? eventTime.hashCode() : 0);
        return result;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
