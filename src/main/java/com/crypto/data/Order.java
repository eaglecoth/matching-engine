package com.crypto.data;

import com.crypto.engine.LimitLevel;

public class Order {
    private Order head;
    private Order tail;
    private CcyPair pair;
    private Side side;
    private long orderId;
    private long size;
    private long clientId;
    private LimitLevel limitLevel;

    public void setTail(Order order){
        tail = order;
    }
    public void setHead(Order order) {
        head = order;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public Order getTail() {
         return tail;
    }

    public long getClientId() {
        return clientId;
    }

    public void setClientId(long clientId) {
        this.clientId = clientId;
    }

    public LimitLevel getLimit() {
        return limitLevel;
    }

    public void setLimit(LimitLevel limitLevel) {
        this.limitLevel = limitLevel;
    }

    public boolean cancelOrder(){
        if(head != null && tail != null){
            tail.setHead(head);
            head.setTail(tail);
            return false;
        }else if(head != null){
            head.setTail(null);
            limitLevel.setOutsideOfBookOrder(head);
            return false;
        }else if(tail != null){
            tail.setHead(null);
            limitLevel.setInsideOfBookOrder(tail);
            return false;
        }else{
            return true;
        }
    }

    public void populate(long id, Message message, LimitLevel limitLevel) {
        this.orderId = id;
        this.limitLevel = limitLevel;
        this.pair = message.getPair();
        this.side = message.getSide();
        this.size = message.getQuantity();
        this.clientId = message.getClientId();
    }
}
