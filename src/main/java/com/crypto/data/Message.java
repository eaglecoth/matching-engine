package com.crypto.data;


/**
 * Representing an instruction type message from clients to the matching engine for processing
 */
public class Message {

    private volatile MessageType type;
    private volatile CcyPair pair;
    private volatile Side side;
    private volatile long quantity;
    private volatile long price;
    private volatile long orderId;
    private volatile long clientId;
    private volatile long clientOrderId;


    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public CcyPair getPair() {
        return pair;
    }

    public void setPair(CcyPair pair) {
        this.pair = pair;
    }

    public long getQuantity() {
        return quantity;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }


    public long getClientId() {
        return clientId;
    }

    public void setClientId(long clientId) {
        this.clientId = clientId;
    }

    public Side getSide() {
        return side;
    }

    public void setSide(Side side) {
        this.side = side;
    }

    public void setClientOrderId(long clientOrderId) {
        this.clientOrderId = clientOrderId;
    }

    public long getClientOrderId() {
        return clientOrderId;
    }

    public void populateFields(Message message){
        this.type = message.getType();
        this.pair = message.getPair();
        this.side = message.getSide();
        this.quantity = message.getQuantity();
        this.price = message.getPrice();
        this.orderId = message.getOrderId();
        this.clientId = message.getClientId();
        this.clientOrderId = message.getClientOrderId();
    }

    @Override
    public String toString() {
        return "Message{" +
                "type=" + type +
                ", pair=" + pair +
                ", side=" + side +
                ", quantity=" + quantity +
                ", price=" + price +
                ", orderId=" + orderId +
                ", clientId=" + clientId +
                ", clientOrderId=" + clientOrderId +
                '}';
    }
}
