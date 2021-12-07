package com.crypto.engine;

import com.crypto.data.*;
import com.crypto.feed.ObjectPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;

public class MatchingEngineIntegrationTest {

    private OrderBookDistributor orderBookDistributor;
    private OrderBookProcessor btcBidProcessor;
    private OrderBookProcessor btcOfferProcessor;
    private OrderBookProcessor ethBidProcessor;
    private OrderBookProcessor ethOfferProcessor;
    private ConcurrentLinkedQueue<Message> distributorInboundQueue;
    private ConcurrentLinkedQueue<Execution> executionPublishQueue;

    @Before
    public void setup(){
        AtomicLong orderIdCounter = new AtomicLong(0);
        ObjectPool<Message> messagePool = new ObjectPool<>(Message::new);
        ObjectPool<Order> orderPool = new ObjectPool<>(Order::new);
        ObjectPool<Execution> executionPool = new ObjectPool<>(Execution::new);

        ConcurrentHashMap<Long, List<Order>> clientIdToOrderMap = new ConcurrentHashMap<>();
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
        int client1LimitOrderId = 1;
        int client2LimitOrderId = 2;
        int clientOrderId = 1;


        Message message = prepareMessage(client1LimitOrderId, clientOrderId, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertExecution(client1LimitOrderId, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.OrderAccepted);


        message = prepareMessage(client2LimitOrderId, clientOrderId, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(2, 2);

        assertExecution(client2LimitOrderId, CcyPair.BTCUSD, 1, 100, Side.Offer, ExecutionType.Fill);
        assertExecution(client1LimitOrderId, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.Fill);

    }

    @Test
    public void testCancel() throws InterruptedException {

        //Base case, check a limit order can be cancelled
        int clientId1 = 1;
        int clientId2 = 2;
        int clientOrderId = 7;
        Message message = prepareMessage(1,clientOrderId, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        long orderId = assertExecution(clientId1, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareCancel(orderId);
        distributorInboundQueue.add(message);
        waitAndAssert(1, 2);
        assertCancel(clientId1,orderId, clientOrderId);


        //Check that a market order gets rejected for its full amount
        message = prepareMessage(clientId2,1, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertReject(clientId2,orderId, 1, 250);



        //Repeat test to check that engine is not in inconsistent state
        message = prepareMessage(1,clientOrderId, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        orderId = assertExecution(clientId1, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareCancel(orderId);
        distributorInboundQueue.add(message);
        waitAndAssert(1, 2);
        assertCancel(clientId1,orderId, clientOrderId);


        //Check that a market order gets rejected for its full amount
        message = prepareMessage(clientId2,1, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertReject(clientId2,orderId, 1, 250);
    }

    @Test
    public void testDoubleLimitVsBigMarket() throws InterruptedException {

        //Test that a market order can partially fill on multiple limit orders
        int clientId1 = 1;
        int clientId2 = 2;
        int clientId3 = 3;

        Message message = prepareMessage(clientId1,1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);

        assertExecution(clientId1, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareMessage(clientId2,1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 1000);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertExecution(clientId2, CcyPair.BTCUSD, 1, 1000, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareMessage(clientId3,1, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        waitAndAssert(4, 2);

        assertExecution( clientId3, CcyPair.BTCUSD, 1, 100, Side.Offer, ExecutionType.PartialFill);
        assertExecution( clientId1, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.Fill);
        assertExecution( clientId3, CcyPair.BTCUSD, 1, 150, Side.Offer, ExecutionType.Fill);
        assertExecution( clientId2, CcyPair.BTCUSD, 1, 150, Side.Bid, ExecutionType.PartialFill);

    }

    @Test
    public void testDoubleLimitPricesVsMarket() throws InterruptedException {

        //Test that a market order matches with the best price first even though it arrived last
        int clientId1 = 1;
        int clientId2 = 2;
        int clientId3 = 3;

        Message message = prepareMessage(clientId1,1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 2, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertExecution(clientId1, CcyPair.BTCUSD, 2, 100, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareMessage(clientId2,1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 1000);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertExecution(clientId2, CcyPair.BTCUSD, 1, 1000, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareMessage(clientId3,1, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        waitAndAssert(4, 2);

        assertExecution( clientId3, CcyPair.BTCUSD, 2, 100, Side.Offer, ExecutionType.PartialFill);
        assertExecution( clientId1, CcyPair.BTCUSD, 2, 100, Side.Bid, ExecutionType.Fill);
        assertExecution( clientId3, CcyPair.BTCUSD, 1, 150, Side.Offer, ExecutionType.Fill);
        assertExecution( clientId2, CcyPair.BTCUSD, 1, 150, Side.Bid, ExecutionType.PartialFill);

    }

    @Test
    public void testDoubleLimitPricesLiftVsMarketBid() throws InterruptedException {

        int client1LimitOrderId = 1;
        int client2LimitOrderId = 2;
        int marketOrderClientId = 3;

        Message message = prepareMessage(client1LimitOrderId,1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 1000);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertExecution(client1LimitOrderId, CcyPair.BTCUSD, 1, 1000, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareMessage(client2LimitOrderId,1, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 2, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertExecution(client2LimitOrderId, CcyPair.BTCUSD, 2, 100, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareMessage(marketOrderClientId,1, CcyPair.BTCUSD, Side.Offer, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        waitAndAssert(4, 2);

        assertExecution( marketOrderClientId, CcyPair.BTCUSD, 2, 100, Side.Offer, ExecutionType.PartialFill);
        assertExecution( client2LimitOrderId, CcyPair.BTCUSD, 2, 100, Side.Bid, ExecutionType.Fill);
        assertExecution( marketOrderClientId, CcyPair.BTCUSD, 1, 150, Side.Offer, ExecutionType.Fill);
        assertExecution( client1LimitOrderId, CcyPair.BTCUSD, 1, 150, Side.Bid, ExecutionType.PartialFill);

    }

    @Test
    public void testDoubleLimitPricesLiftVsMarketOffer() throws InterruptedException {

        int client1LimitOrderId = 1;
        int client2LimitOrderId = 2;
        int marketOrderClientId = 3;

        Message message = prepareMessage(client1LimitOrderId,1, CcyPair.BTCUSD, Side.Offer, MessageType.NewLimitOrder, 2, 1000);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertExecution(client1LimitOrderId, CcyPair.BTCUSD, 2, 1000, Side.Offer, ExecutionType.OrderAccepted);

        message = prepareMessage(client2LimitOrderId,1, CcyPair.BTCUSD, Side.Offer, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        assertExecution(client2LimitOrderId, CcyPair.BTCUSD, 1, 100, Side.Offer, ExecutionType.OrderAccepted);

        message = prepareMessage(marketOrderClientId,1, CcyPair.BTCUSD, Side.Bid, MessageType.NewMarketOrder, 1, 250);
        distributorInboundQueue.add(message);

        waitAndAssert(4, 2);

        assertExecution( marketOrderClientId, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.PartialFill);
        assertExecution( client2LimitOrderId, CcyPair.BTCUSD, 1, 100, Side.Offer, ExecutionType.Fill);
        assertExecution( marketOrderClientId, CcyPair.BTCUSD, 2, 150, Side.Bid, ExecutionType.Fill);
        assertExecution( client1LimitOrderId, CcyPair.BTCUSD, 2, 150, Side.Offer, ExecutionType.PartialFill);

    }

    @Test
    public void testMassCancel() throws InterruptedException {

        //Base case, check a limit order can be cancelled
        int clientId1 = 1;
        int clientId2 = 2;
        int clientOrderId = 7;
        long clientOrderId2 = 2;
        long orderId1 = 0;
        long orderId2 = 0;
        long orderId3 = 0;
        long orderId4 = 0;
        long orderId5 = 0;
        Message message = prepareMessage(clientId1,clientOrderId, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        orderId1 = assertExecution(clientId1, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareMessage(clientId2,2, CcyPair.ETHUSD, Side.Offer, MessageType.NewLimitOrder, 3, 250);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        orderId2 = assertExecution(clientId2, CcyPair.ETHUSD, 3, 250, Side.Offer, ExecutionType.OrderAccepted);


        message = prepareMessage(clientId2,clientOrderId, CcyPair.BTCUSD, Side.Bid, MessageType.NewLimitOrder, 1, 100);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        orderId3 = assertExecution(clientId2, CcyPair.BTCUSD, 1, 100, Side.Bid, ExecutionType.OrderAccepted);

        message = prepareMessage(clientId1,clientOrderId2, CcyPair.ETHUSD, Side.Offer, MessageType.NewLimitOrder, 3, 250);
        distributorInboundQueue.add(message);

        waitAndAssert(1, 2);
        orderId4 = assertExecution(clientId1, CcyPair.ETHUSD, 3, 250, Side.Offer, ExecutionType.OrderAccepted);


        message = prepareMessage(clientId1,0, null, null, MessageType.CancelAllOrders, 0, 0);
        distributorInboundQueue.add(message);

        waitAndAssert(2, 2);
        Execution cancelMsg = executionPublishQueue.poll();
        assertEquals(ExecutionType.CancelAccepted, cancelMsg.getType());
        assertEquals(clientId1, cancelMsg.getClientId());
        executionPublishQueue.poll();
        assertEquals(ExecutionType.CancelAccepted, cancelMsg.getType());
        assertEquals(clientId1, cancelMsg.getClientId());

        message = prepareMessage(clientId2,0, null, null, MessageType.CancelAllOrders, 0, 0);
        distributorInboundQueue.add(message);

        waitAndAssert(2, 2);
        cancelMsg = executionPublishQueue.poll();
        assertEquals(ExecutionType.CancelAccepted, cancelMsg.getType());
        assertEquals(clientId2, cancelMsg.getClientId());
        executionPublishQueue.poll();
        assertEquals(ExecutionType.CancelAccepted, cancelMsg.getType());
        assertEquals(clientId2, cancelMsg.getClientId());


    }

    private void waitAndAssert(int expectedMessages, int waitCount) throws InterruptedException {

        Thread.sleep(50);
        while ( executionPublishQueue.size() < expectedMessages && waitCount > 0){
            System.out.println("Waiting for Execution to arrive...");
            Thread.sleep(100);
            waitCount -=1;
        }
        assertEquals("Two Executions Expected", expectedMessages, executionPublishQueue.size());

    }

    private void assertReject(int clientId,long orderId, long clientOrderId, long size) {
        Execution execution = executionPublishQueue.poll();
        assertEquals(clientId, execution.getClientId());
        assertEquals(clientOrderId, execution.getClientOrderId());
        assertEquals(size, execution.getQuantity());
        assertEquals(ExecutionType.Reject, execution.getType());
    }

    private void assertCancel(int clientId,long orderId, long clientOrderId) {
        Execution execution = executionPublishQueue.poll();
        assertEquals(clientId, execution.getClientId());
        assertEquals(orderId, execution.getOrderId());
        assertEquals(clientOrderId, execution.getClientOrderId());
        assertEquals(ExecutionType.CancelAccepted, execution.getType());
    }

    private long assertExecution(int clientId, CcyPair pair, long price, long quantity, Side side, ExecutionType type) {
        Execution execution = executionPublishQueue.poll();
        assertEquals(clientId, execution.getClientId());
        assertEquals(pair, execution.getPair());
        assertEquals(price, execution.getPrice());
        assertEquals(quantity, execution.getQuantity());
        assertEquals(side, execution.getSide());
        assertEquals(type, execution.getType());

        return execution.getOrderId();
    }

    private Message prepareCancel(long orderId){
        Message message = new Message();
        message.setOrderId(orderId);
        message.setType(MessageType.CancelOrder);
        return message;
    }

    private Message prepareMessage(long clientId, long clientOrderId, CcyPair pair, Side side, MessageType type, long price, long quantity){
        Message message = new Message();

        message.setClientId(clientId);
        message.setClientOrderId(clientOrderId);
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