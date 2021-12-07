package com.crypto.data;

import com.crypto.data.CcyPair;
import com.crypto.data.Side;

public class Execution {
    private ExecutionType type;
    private long clientId;
    private long quantity;
    private long price;
    private CcyPair pair;
    private Side side;
    private long orderId;
    private long clientOrderId;

    public void setClientId(long clientId) {
        this.clientId = clientId;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;

    }

    public void setCcyPair(CcyPair pair) {
        this.pair = pair;

    }

    public void setPrice(long price) {
        this.price = price;
    }

    public long getClientId() {
        return clientId;
    }

    public long getQuantity() {
        return quantity;
    }

    public long getPrice() {
        return price;
    }

    public CcyPair getPair() {
        return pair;
    }

    public void setPair(CcyPair pair) {
        this.pair = pair;
    }

    public Side getSide() {
        return side;
    }

    public void setSide(Side side) {
        this.side = side;
    }

    public ExecutionType getType() {
        return type;
    }

    public void setType(ExecutionType type) {
        this.type = type;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public long getClientOrderId() {
        return clientOrderId;
    }

    public void setClientOrderId(long clientOrderId) {
        this.clientOrderId = clientOrderId;
    }
}
