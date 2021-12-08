package com.crypto.engine;

import com.crypto.data.*;
import com.crypto.feed.ObjectPool;

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
     * @param limitToInsert new price level to be added
     * @param currentLimitLevel limit price level to compare to, normally start at top of book
     */
    protected void insertInChain(LimitLevel limitToInsert, LimitLevel currentLimitLevel) {
            if (limitToInsert.getPrice() > currentLimitLevel.getPrice()) {
                LimitLevel limitAboveCurrent = currentLimitLevel.getNextHigher();
                if (limitAboveCurrent == null) {
                    //We're inserting a new best price
                    currentLimitLevel.setNextHigher(limitToInsert);
                    limitToInsert.setNextLower(currentLimitLevel);
                    topOfBook = limitToInsert;
                }else{
                    //We're inserting a new price somewhere in the middle of the book
                    limitAboveCurrent.setNextLower(limitToInsert);
                    limitToInsert.setNextHigher(limitAboveCurrent);
                    limitToInsert.setNextLower(currentLimitLevel);
                    currentLimitLevel.setNextHigher(currentLimitLevel);
                }
                return;

            } else if (currentLimitLevel.getNextLower() == null) {
                //We're inserting a price at the bottom of the book
                currentLimitLevel.setNextLower(limitToInsert);
                limitToInsert.setNextHigher(currentLimitLevel);
                return;
            }
            //Recurse and step to the next limit in the book
            insertInChain(limitToInsert, currentLimitLevel.getNextLower());
    }
}
