package com.crypto.application;

import com.crypto.data.CcyPair;
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
        MessageSerializer serializer = new MessageSerializerImpl(distributorInboundQueue, messagePool, 3, 100, ";");

        ConcurrentHashMap<Long, List<Order>> clientIdToOrderMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Order> idToOrderMap =  new ConcurrentHashMap<>();

        MatchingEngineProcessor orderBookDistributor = new OrderBookDistributor(queues, clientIdToOrderMap, messagePool, idToOrderMap);
        OrderBookProcessor btcBidProcessor = new BidOrderBookProcessor(CcyPair.BTCUSD, orderPool, executionPool, messagePool, distributorInboundQueue, executionPublishQueue, orderIdCounter, idToOrderMap);
        OrderBookProcessor btcOfferProcessor = new OfferOrderBookProcessor(CcyPair.BTCUSD, orderPool, executionPool, messagePool, distributorInboundQueue, executionPublishQueue, orderIdCounter, idToOrderMap);
        OrderBookProcessor ethBidProcessor = new BidOrderBookProcessor(CcyPair.ETHUSD, orderPool, executionPool, messagePool, distributorInboundQueue, executionPublishQueue, orderIdCounter, idToOrderMap);
        OrderBookProcessor ethOfferProcessor = new OfferOrderBookProcessor(CcyPair.ETHUSD, orderPool, executionPool, messagePool, distributorInboundQueue, executionPublishQueue, orderIdCounter, idToOrderMap);


    }
}
