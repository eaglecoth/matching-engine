package com.crypto.engine;

import com.crypto.data.CcyPair;
import com.crypto.data.Message;
import com.crypto.data.Order;
import com.crypto.feed.ObjectPool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class BidOrderBookProcessor extends OrderBookProcessor{

    public BidOrderBookProcessor(CcyPair pair, ObjectPool<Order> orderObjectPool, ObjectPool<Execution> executionObjectPool, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Execution> executionPublishQueue, AtomicLong orderCounter, ConcurrentHashMap<Long, Order> idToOrderMap) {
        super(pair, orderObjectPool, executionObjectPool, messageObjectPool, distributorInboundQueue, executionPublishQueue, orderCounter,  idToOrderMap);
    }

    @Override
    protected LimitLevel getNextLevelLimit(LimitLevel limitLevelToExecute) {
        return limitLevelToExecute.getNextLower();
    }

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
