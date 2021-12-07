package com.crypto.engine;

import com.crypto.data.*;
import com.crypto.feed.ObjectPool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class OfferOrderBookProcessor extends OrderBookProcessor{

    public OfferOrderBookProcessor(CcyPair pair, ObjectPool<Order> orderObjectPool, ObjectPool<Execution> executionObjectPool, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Execution> executionPublishQueue, AtomicLong orderCounter) {
        super(pair, orderObjectPool, executionObjectPool, messageObjectPool, distributorInboundQueue, executionPublishQueue, orderCounter);

    }

    @Override
    protected Side getSide() {
        return Side.Offer;
    }

    @Override
    protected Side getOppositeSide() {
        return Side.Bid;
    }

    @Override
    protected LimitLevel getNextLevelLimit(LimitLevel limitLevelToExecute) {
        return limitLevelToExecute.getNextHigher();
    }

    protected void insertInChain(LimitLevel newLimitLevel, LimitLevel currentLimitLevel) {
        if (newLimitLevel.getPrice() < currentLimitLevel.getPrice()) {
            LimitLevel newLower = currentLimitLevel.getNextLower();
            if (newLower == null) {
                currentLimitLevel.setNextLower(newLimitLevel);
                newLimitLevel.setNextHigher(currentLimitLevel);
            }else{
                LimitLevel newLowerLimitLevel = currentLimitLevel.getNextLower();
                newLowerLimitLevel.setNextHigher(currentLimitLevel);
                newLimitLevel.setNextLower(newLowerLimitLevel);
                newLimitLevel.setNextHigher(currentLimitLevel);
                currentLimitLevel.setNextLower(newLimitLevel);
            }
            return;

        } else if (currentLimitLevel.getNextHigher() == null) {
            currentLimitLevel.setNextHigher(newLimitLevel);
            newLimitLevel.setNextLower(currentLimitLevel);
            return;
        }
        insertInChain(newLimitLevel, currentLimitLevel.getNextLower());
    }

    @Override
    void reevaluateTopOfBook(LimitLevel newLimitLevel) {
        if(newLimitLevel.getPrice() < topOfBook.getPrice()){
            topOfBook = newLimitLevel;
        }
    }
}
