package com.crypto.engine;

import com.crypto.data.*;
import com.crypto.feed.MessageSerializer;
import com.crypto.feed.MessageSerializerImpl;
import com.crypto.feed.ObjectPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.crypto.data.Constants.MESSAGE_DELIMITER;
import static org.junit.Assert.*;

public class OrderBookDistributorTest {

    private OrderBookDistributor orderBookDistributor;
    private OrderBookProcessor btcBidProcessor;
    private OrderBookProcessor btcOfferProcessor;
    private OrderBookProcessor ethBidProcessor;
    private OrderBookProcessor ethOfferProcessor;
    private ConcurrentLinkedQueue<Message> distributorInboundQueue;
    private ConcurrentLinkedQueue<Execution> executionPublishQueue;
    private ConcurrentHashMap<Long, List<Order>> clientIdToOrderMap;
    private ConcurrentHashMap<Long, Order> idToOrderMap;
    private MessageSerializer serializer;
    private AtomicLong orderIdCounter;
    ObjectPool<Message> messagePool;
    ObjectPool<Order> orderPool;
    ObjectPool<Execution> executionPool;

    @Before
    public void setup(){
        orderIdCounter = new AtomicLong(0);
        messagePool = new ObjectPool<>(Message::new);
        orderPool = new ObjectPool<>(Order::new);
        executionPool = new ObjectPool<>(Execution::new);

        serializer = new MessageSerializerImpl(distributorInboundQueue, messagePool, 3, 100, MESSAGE_DELIMITER);
        clientIdToOrderMap = new ConcurrentHashMap<>();
        idToOrderMap =  new ConcurrentHashMap<>();
        distributorInboundQueue = new ConcurrentLinkedQueue<>();
        executionPublishQueue = new ConcurrentLinkedQueue<>();

        List<ConcurrentLinkedQueue<Message>> queues = new ArrayList<>(4);
        for (int i = 0; i < 4; i++) {
            queues.add(new ConcurrentLinkedQueue<>());
        }

        orderBookDistributor = new OrderBookDistributor(distributorInboundQueue, queues, clientIdToOrderMap, messagePool);
        btcOfferProcessor = new OfferOrderBookProcessor(CcyPair.BTCUSD, orderPool, executionPool, messagePool, queues.get(0), executionPublishQueue, orderIdCounter);
        btcBidProcessor = new BidOrderBookProcessor(CcyPair.BTCUSD, orderPool, executionPool, messagePool, queues.get(1), executionPublishQueue, orderIdCounter);
        ethOfferProcessor = new OfferOrderBookProcessor(CcyPair.ETHUSD, orderPool, executionPool, messagePool, queues.get(2), executionPublishQueue, orderIdCounter);
        ethBidProcessor = new BidOrderBookProcessor(CcyPair.ETHUSD, orderPool, executionPool, messagePool, queues.get(3), executionPublishQueue, orderIdCounter);
    }


    @Test
    public void testSingleLimitVsMarket() throws InterruptedException {

        //Base case, check a limit order and a market order can match

        Message message = prepareMessage(1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        Thread.sleep(100);

        message = prepareMessage(2, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 100);
        distributorInboundQueue.add(message);

        while ( executionPublishQueue.size() < 2){
            System.out.println("Waiting for Execution to arrive...");
            Thread.sleep(100);
        }
        assertEquals("Two Executions Expected", executionPublishQueue.size(), 2);

        Execution execution = executionPublishQueue.poll();
        assertExecution(execution, 2, CcyPair.BTCUSD, 1, 100, Side.Offer);
        execution = executionPublishQueue.poll();
        assertExecution(execution, 1, CcyPair.BTCUSD, 1, 100, Side.Bid);

    }

    @Test
    public void testCancel() throws InterruptedException {

        //Base case, check a limit order and a market order can match

        Message message = prepareMessage(1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        Thread.sleep(100);

        message = prepareMessage(2, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 100);
        distributorInboundQueue.add(message);

        while ( executionPublishQueue.size() < 2){
            System.out.println("Waiting for Execution to arrive...");
            Thread.sleep(100);
        }
        assertEquals("Two Executions Expected", executionPublishQueue.size(), 2);

        Execution execution = executionPublishQueue.poll();
        assertExecution(execution, 2, CcyPair.BTCUSD, 1, 100, Side.Offer);
        execution = executionPublishQueue.poll();
        assertExecution(execution, 1, CcyPair.BTCUSD, 1, 100, Side.Bid);

    }

    @Test
    public void testDoubleLimitVsBigMarket() throws InterruptedException {

        //Test that a market order can partially fill on multiple limit orders

        Message message = prepareMessage(1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        Thread.sleep(100);

        message = prepareMessage(2, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 1000);
        distributorInboundQueue.add(message);

        message = prepareMessage(3, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        while ( executionPublishQueue.size() < 4){
            System.out.println("Waiting for Execution to arrive...");
            Thread.sleep(100);
        }
        assertEquals("Two Executions Expected",4, executionPublishQueue.size());

        Execution execution = executionPublishQueue.poll();
        assertExecution(execution, 3, CcyPair.BTCUSD, 1, 100, Side.Offer);
        execution = executionPublishQueue.poll();
        assertExecution(execution, 1, CcyPair.BTCUSD, 1, 100, Side.Bid);
        execution = executionPublishQueue.poll();
        assertExecution(execution, 3, CcyPair.BTCUSD, 1, 150, Side.Offer);
        execution = executionPublishQueue.poll();
        assertExecution(execution, 2, CcyPair.BTCUSD, 1, 150, Side.Bid);

    }

    @Test
    public void testDoubleLimitPricesVsMarket() throws InterruptedException {

        //Test that a market order matches with the best price first even though it arrived last

        Message message = prepareMessage(1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 2, 100);
        distributorInboundQueue.add(message);

        Thread.sleep(100);

        message = prepareMessage(2, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 1000);
        distributorInboundQueue.add(message);

        message = prepareMessage(3, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        while ( executionPublishQueue.size() < 4){
            System.out.println("Waiting for 4 Execution to arrive but only " + executionPublishQueue.size() + " so far...");
            Thread.sleep(100);
        }
        assertEquals("Two Executions Expected",4, executionPublishQueue.size());

        Execution execution = executionPublishQueue.poll();
        assertExecution(execution, 3, CcyPair.BTCUSD, 2, 100, Side.Offer);
        execution = executionPublishQueue.poll();
        assertExecution(execution, 1, CcyPair.BTCUSD, 2, 100, Side.Bid);
        execution = executionPublishQueue.poll();
        assertExecution(execution, 3, CcyPair.BTCUSD, 1, 150, Side.Offer);
        execution = executionPublishQueue.poll();
        assertExecution(execution, 2, CcyPair.BTCUSD, 1, 150, Side.Bid);

    }

    @Test
    public void testDoubleLimitPricesLiftVsMarket() throws InterruptedException {

        int client1LimitOrderId = 1;
        int client2LimitOrderId = 2;
        int marketOrderClientId = 3;

        Message message = prepareMessage(client1LimitOrderId, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 1000);
        distributorInboundQueue.add(message);

        Thread.sleep(100);

        message = prepareMessage(client2LimitOrderId, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 2, 100);
        distributorInboundQueue.add(message);

        message = prepareMessage(marketOrderClientId, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        while ( executionPublishQueue.size() < 4){
            System.out.println("Waiting for 4 Execution to arrive but only " + executionPublishQueue.size() + " so far...");
            Thread.sleep(100);
        }
        assertEquals("Two Executions Expected",4, executionPublishQueue.size());

        Execution execution = executionPublishQueue.poll();
        assertExecution(execution, marketOrderClientId, CcyPair.BTCUSD, 2, 100, Side.Offer);
        execution = executionPublishQueue.poll();
        assertExecution(execution, client2LimitOrderId, CcyPair.BTCUSD, 2, 100, Side.Bid);
        execution = executionPublishQueue.poll();
        assertExecution(execution, marketOrderClientId, CcyPair.BTCUSD, 1, 150, Side.Offer);
        execution = executionPublishQueue.poll();
        assertExecution(execution, client1LimitOrderId, CcyPair.BTCUSD, 1, 150, Side.Bid);

    }

    private void assertExecution(Execution execution, int clientId, CcyPair pair, long price, long quantity, Side side) {
        assertEquals(clientId, execution.getClientId());
        assertEquals(pair, execution.getPair());
        assertEquals(price, execution.getPrice());
        assertEquals(quantity, execution.getQuantity());
        assertEquals(side, execution.getSide() );
    }

    private Message prepareMessage(long clientId, CcyPair pair, Side side, MessageType type, long price, long quantity){
        Message message = new Message();

        message.setClientId(clientId);
        message.setPair(pair);
        message.setSide(side);
        message.setType(type);
        message.setPrice(price);
        message.setQuantity(quantity);

        return message;

    }


    @After
    public void tearDown() throws Exception {
        orderBookDistributor.shutdown();
        btcBidProcessor.shutdown();
        btcOfferProcessor.shutdown();
        ethBidProcessor.shutdown();
        ethOfferProcessor.shutdown();
    }
}