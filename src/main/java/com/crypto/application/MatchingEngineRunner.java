package com.crypto.application;

import com.crypto.data.CcyPair;
import com.crypto.data.Execution;
import com.crypto.data.Message;
import com.crypto.data.Order;
import com.crypto.engine.*;
import com.crypto.feed.ObjectPool;
import com.crypto.feed.MessageSerializer;
import com.crypto.feed.MessageSerializerImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.crypto.data.Constants.MESSAGE_DELIMITER;
import static com.crypto.data.Constants.NEW_LIMIT_ORDER;


/**
 * Main running class to test the matching engine
 */
public class MatchingEngineRunner {

    public static void main(String[] args) {

        List<ConcurrentLinkedQueue<Message>> queues = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            queues.add(new ConcurrentLinkedQueue<>());
        }

        AtomicLong orderIdCounter = new AtomicLong(0);
        ObjectPool<Message> messagePool = new ObjectPool<>(Message::new);
        ObjectPool<Order> orderPool = new ObjectPool<>(Order::new);
        ObjectPool<Execution> executionPool = new ObjectPool<>(Execution::new);


        ConcurrentLinkedQueue<Message> distributorInboundQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Execution> executionPublishQueue = new ConcurrentLinkedQueue<>();
        MessageSerializer serializer = new MessageSerializerImpl(distributorInboundQueue, messagePool, 3, 100, MESSAGE_DELIMITER);

        ConcurrentHashMap<Long, List<Order>> clientIdToOrderMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Order> idToOrderMap =  new ConcurrentHashMap<>();

        OrderBookDistributor orderBookDistributor = new OrderBookDistributor(distributorInboundQueue, queues, clientIdToOrderMap, messagePool);

        OrderBookProcessor btcBidProcessor = new BidOrderBookProcessor(CcyPair.BTCUSD, orderPool, executionPool, messagePool, distributorInboundQueue, executionPublishQueue, orderIdCounter);
        OrderBookProcessor btcOfferProcessor = new OfferOrderBookProcessor(CcyPair.BTCUSD, orderPool, executionPool, messagePool, distributorInboundQueue, executionPublishQueue, orderIdCounter);
        OrderBookProcessor ethBidProcessor = new BidOrderBookProcessor(CcyPair.ETHUSD, orderPool, executionPool, messagePool, distributorInboundQueue, executionPublishQueue, orderIdCounter);
        OrderBookProcessor ethOfferProcessor = new OfferOrderBookProcessor(CcyPair.ETHUSD, orderPool, executionPool, messagePool, distributorInboundQueue, executionPublishQueue, orderIdCounter);


        serializer.onMessage(NEW_LIMIT_ORDER);


    }
}
