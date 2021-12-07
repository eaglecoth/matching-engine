package com.crypto.engine;

import com.crypto.data.Execution;
import com.crypto.data.Order;
import com.crypto.feed.ObjectPool;

public class LimitLevel {

    private OrderBookProcessor processor;
    private Order insideOfBookOrder;
    private Order outsideOfBookOrder;
    private LimitLevel nextHigher;
    private LimitLevel nextLower;
    ObjectPool<Execution> executionObjectPool;
    private long price;

    public LimitLevel(){}

    public void populate(Long price, ObjectPool<Execution> executionObjectPool, OrderBookProcessor processor) {

        this.executionObjectPool = executionObjectPool;
        this.insideOfBookOrder = null;
        this.outsideOfBookOrder = null;
        this.price = price;
        this.processor = processor;
    }

    /**
     * Method will link in new order to the price level linked list
     * @param order order to add to limit
     */
    public void addOrder(Order order){
        if(insideOfBookOrder == null){
            insideOfBookOrder = order;
            outsideOfBookOrder = order;
            order.setHead(null);
            order.setTail(null);
        }else{
            outsideOfBookOrder.setTail(order);
            order.setHead(outsideOfBookOrder);
            order.setTail(null);
            outsideOfBookOrder = order;
        }
    }

    public long getPrice() {
        return price;
    }

    public LimitLevel getNextHigher() {
        return nextHigher;
    }

    public LimitLevel getNextLower() {
        return nextLower;
    }

    public void setNextHigher(LimitLevel nextHigher) {
        this.nextHigher = nextHigher;
    }

    public void setNextLower(LimitLevel nextLower) {
        this.nextLower = nextLower;
    }

    public Order peekInsideOfBook(){
        return insideOfBookOrder;
    }

    /**
     * Remove the head of the list of the book and replace with its follower
     * @return the order sitting inside of book.
     */
    public Order pollInsideOfBook(){

        Order newInsideOfBook = insideOfBookOrder.getTail();
        Order orderToRemove = insideOfBookOrder;
        if(newInsideOfBook != null){
            newInsideOfBook.setHead(null);
        }
        insideOfBookOrder = newInsideOfBook;
        return orderToRemove;
    }

    /**
     * If all orders are either filled or cancelled, the limit does no longer serve any purpose and should be removed
     * from book
     * @return true if this limit was the last in the book
     */
    public boolean removeLimitFromOrderbook(){
        if(nextHigher != null && nextLower != null){
            nextHigher.setNextLower(nextLower);
            nextLower.setNextHigher(nextHigher);
            return false;
        }else if(nextHigher != null){
            nextHigher.setNextLower(null);
            return false;
        }else if(nextLower != null){
            nextLower.setNextHigher(null);
            return false;
        }else{
            return true;
        }
    }

    public OrderBookProcessor getProcessor() {
        return processor;
    }

    public void setInsideOfBookOrder(Order insideOfBookOrder) {
        this.insideOfBookOrder = insideOfBookOrder;
    }

    public void setOutsideOfBookOrder(Order outsideOfBookOrder) {
        this.outsideOfBookOrder = outsideOfBookOrder;
    }

    public boolean isEmpty() {
        return peekInsideOfBook() == null;
    }
}
