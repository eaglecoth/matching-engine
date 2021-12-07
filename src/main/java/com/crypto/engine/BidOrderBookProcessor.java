package com.crypto.engine;

import com.crypto.data.*;
import com.crypto.feed.ObjectPool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of the Bid side of an order book
 */
public class BidOrderBookProcessor extends OrderBookProcessor{

    public BidOrderBookProcessor(CcyPair pair, ObjectPool<Order> orderObjectPool, ObjectPool<Execution> executionObjectPool, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Execution> executionPublishQueue, AtomicLong orderCounter) {
        super(pair, orderObjectPool, executionObjectPool, messageObjectPool, distributorInboundQueue, executionPublishQueue, orderCounter);
    }

    @Override
    protected boolean priceCrossingSpread(long price) {
        return price >= correspondingProcessor.getTopOfBookPrice();
    }

    @Override
    protected Side getSide() {
        return Side.Bid;
    }

    @Override
    protected Side getOppositeSide() {
        return Side.Offer;
    }

    @Override
    protected long getTopOfBookPrice() {
        return topOfBook == null ? 0 : topOfBook.getPrice();
    }

    @Override
    protected LimitLevel getNextLevelLimit(LimitLevel limitLevelToExecute) {
        return limitLevelToExecute.getNextLower();
    }

    /**
     * Limits are ordered in a sorted double linked list. When a new limit arrives, we traverse the list and insert
     * at the appropriate spot
     * @param newLimitLevel new price level to be added
     * @param currentLimitLevel limit price level to compare to, normally start at top of book
     */
    protected void insertInChain(LimitLevel newLimitLevel, LimitLevel currentLimitLevel) {
            if (newLimitLevel.getPrice() > currentLimitLevel.getPrice()) {
                LimitLevel newHigher = currentLimitLevel.getNextHigher();
                if (newHigher == null) {
                    currentLimitLevel.setNextHigher(newLimitLevel);
                    newLimitLevel.setNextLower(currentLimitLevel);
                }else{
                    newHigher.setNextLower(newLimitLevel);
                    newLimitLevel.setNextHigher(newHigher);
                    newLimitLevel.setNextLower(currentLimitLevel);
                    currentLimitLevel.setNextHigher(currentLimitLevel);
                }
                return;

            } else if (currentLimitLevel.getNextLower() == null) {
                currentLimitLevel.setNextLower(newLimitLevel);
                newLimitLevel.setNextHigher(currentLimitLevel);
                return;
            }
            insertInChain(newLimitLevel, currentLimitLevel.getNextLower());
    }

    @Override
    void reevaluateTopOfBook(LimitLevel newLimitLevel) {

        if(newLimitLevel.getPrice() > topOfBook.getPrice()){
            topOfBook = newLimitLevel;
        }
    }




}
