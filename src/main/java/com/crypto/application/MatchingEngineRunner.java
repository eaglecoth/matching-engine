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

import static com.crypto.data.Constants.*;


/**
 * Main running class to test the matching engine. Some sample orders being
 */
public class MatchingEngineRunner {

    public static void main(String[] args) throws InterruptedException {

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
        OrderBookDistributor orderBookDistributor = new OrderBookDistributor(distributorInboundQueue, queues, clientIdToOrderMap, messagePool);


        OrderBookProcessor btcOfferProcessor = new OfferOrderBookProcessor(CcyPair.BTCUSD, orderPool, executionPool, messagePool, queues.get(0), executionPublishQueue, orderIdCounter);
        OrderBookProcessor btcBidProcessor = new BidOrderBookProcessor(CcyPair.BTCUSD, orderPool, executionPool, messagePool, queues.get(1), executionPublishQueue, orderIdCounter);
        OrderBookProcessor ethBidProcessor = new BidOrderBookProcessor(CcyPair.ETHUSD, orderPool, executionPool, messagePool, queues.get(2), executionPublishQueue, orderIdCounter);
        OrderBookProcessor ethOfferProcessor = new OfferOrderBookProcessor(CcyPair.ETHUSD, orderPool, executionPool, messagePool, queues.get(3), executionPublishQueue, orderIdCounter);

        btcOfferProcessor.setCorrespondingBook(btcBidProcessor);
        btcBidProcessor.setCorrespondingBook(btcOfferProcessor);
        ethOfferProcessor.setCorrespondingBook(ethBidProcessor);
        ethBidProcessor.setCorrespondingBook(ethOfferProcessor);

        btcOfferProcessor.startOrderBook();
        btcBidProcessor.startOrderBook();
        ethOfferProcessor.startOrderBook();
        ethBidProcessor.startOrderBook();


        String limitOrder = getLimitOrder("666", "123", "100", BID, BTCUSD, "10000");


        serializer.onMessage(limitOrder);
        Thread.sleep(200);
        if(executionPublishQueue.size() == 1){
            System.out.println("Something came back: " + executionPublishQueue.poll());
        }

        String marketOrder = getMarketOrder("667", "321", OFFER, BTCUSD, "500");

        serializer.onMessage(marketOrder);
        Thread.sleep(200);

        if(executionPublishQueue.size() == 2){
            System.out.println("Something came back: " + executionPublishQueue.poll());
            System.out.println("Something came back: " + executionPublishQueue.poll());
        }

        orderBookDistributor.shutdown();
        btcBidProcessor.shutdown();
        btcOfferProcessor.shutdown();
        ethBidProcessor.shutdown();
        ethOfferProcessor.shutdown();

    }

    private static String getLimitOrder(String clientId, String clientOrderId, String price, String side, String ccy , String quantity) {
        return NEW_LIMIT_ORDER + MESSAGE_DELIMITER + clientId +MESSAGE_DELIMITER + clientOrderId + MESSAGE_DELIMITER + ccy + MESSAGE_DELIMITER + side + MESSAGE_DELIMITER + quantity + MESSAGE_DELIMITER + price;
    }

    private static String getMarketOrder(String clientId, String clientOrderId, String side, String ccy, String quantity  ) {
        return NEW_MARKET_ORDER + MESSAGE_DELIMITER + clientId +MESSAGE_DELIMITER + clientOrderId + MESSAGE_DELIMITER + ccy + MESSAGE_DELIMITER + side + MESSAGE_DELIMITER + quantity;
    }
}
