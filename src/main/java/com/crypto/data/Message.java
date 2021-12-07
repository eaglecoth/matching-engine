package com.crypto.data;


/**
 * Representing an instruction type message from Clients to the matching engine
 */
public class Message {

    private MessageType type;
    private CcyPair pair;
    private Side side;
    private long quantity;
    private long price;
    private long orderId;
    private long clientId;
    private long clientOrderId;


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

    public void setLimit(Long valueOf) {
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
}
