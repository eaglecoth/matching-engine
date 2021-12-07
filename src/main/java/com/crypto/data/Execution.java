package com.crypto.data;

import com.crypto.data.CcyPair;
import com.crypto.data.Side;

public class Execution {
    private volatile ExecutionType type;
    private volatile long clientId;
    private volatile long quantity;
    private volatile long price;
    private volatile CcyPair pair;
    private volatile Side side;
    private volatile long orderId;
    private volatile long clientOrderId;

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

    @Override
    public String toString() {
        return "Execution{" +
                "type=" + type +
                ", clientId=" + clientId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", pair=" + pair +
                ", side=" + side +
                ", orderId=" + orderId +
                ", clientOrderId=" + clientOrderId +
                '}';
    }
}
