package com.crypto.engine;

import com.crypto.data.CcyPair;
import com.crypto.data.Message;
import com.crypto.data.Order;
import com.crypto.feed.ObjectPool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class OfferOrderBookProcessor extends OrderBookProcessor{

    public OfferOrderBookProcessor(CcyPair pair, ObjectPool<Order> orderObjectPool, ObjectPool<Execution> executionObjectPool,  ObjectPool<Message> messageObjectPool,ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Execution> executionPublishQueue, AtomicLong orderCounter, ConcurrentHashMap<Long, Order> idToOrderMap) {
        super(pair, orderObjectPool, executionObjectPool, messageObjectPool, distributorInboundQueue, executionPublishQueue, orderCounter,idToOrderMap);

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
