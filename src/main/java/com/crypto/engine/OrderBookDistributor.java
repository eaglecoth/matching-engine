package com.crypto.engine;

import com.crypto.data.CcyPair;
import com.crypto.data.Message;
import com.crypto.data.MessageType;
import com.crypto.data.Order;
import com.crypto.feed.ObjectPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class OrderBookDistributor implements MatchingEngineProcessor {

    private volatile boolean runningFlag = true;
    private Map<Long, LimitLevel> priceLimitIndex;
    private ConcurrentHashMap<Long, List<Order>> clientIdToOrderMap;
    private ConcurrentHashMap<Long, Order> idToOrderMap;
    ConcurrentLinkedQueue<Message> incomingMessageQueue;
    ConcurrentLinkedQueue<Message> btcUsdOfferBookQueue;
    ConcurrentLinkedQueue<Message> btcUsdBidBookQueue;
    ConcurrentLinkedQueue<Message> ethUsdOfferBookQueue;
    ConcurrentLinkedQueue<Message> ethUsdBidBookQueue;
    ObjectPool<Message> messagePool;
    AtomicLong orderCounter;

    public OrderBookDistributor(List<ConcurrentLinkedQueue<Message>> engineMessageQueue, ConcurrentHashMap<Long, List<Order>> clientIdToOrderMap, ObjectPool<Message> messagePool, ConcurrentHashMap<Long, Order> idToOrderMap) {

        orderCounter = new AtomicLong(0);
        this.messagePool = messagePool;
        this.clientIdToOrderMap = clientIdToOrderMap;

        incomingMessageQueue = engineMessageQueue.get(0);
        btcUsdOfferBookQueue = engineMessageQueue.get(1);
        btcUsdBidBookQueue = engineMessageQueue.get(2);
        ethUsdOfferBookQueue = engineMessageQueue.get(3);
        ethUsdBidBookQueue = engineMessageQueue.get(4);

        Thread thread = new Thread(() -> {
            System.out.println("Order Book Distributor Running");

            while (runningFlag) {
                Message message = incomingMessageQueue.poll();
                if (message != null) {
                    processMessage(message);
                }
            }
        });

        thread.start();
    }

    private void processMessage(Message message) {

        switch (message.getType()) {

            case NewMarketOrder:
            case NewLimitOrder:
                switch (message.getSide()) {
                    case Bid:
                        switch (message.getPair()) {
                            case ETHUSD:
                                ethUsdBidBookQueue.add(message);
                                return;
                            case BTCUSD:
                                btcUsdOfferBookQueue.add(message);
                                return;
                        }

                    case Offer:
                        switch (message.getPair()) {
                            case ETHUSD:
                                ethUsdOfferBookQueue.add(message);
                                return;
                            case BTCUSD:
                                btcUsdBidBookQueue.add(message);
                                return;
                        }

                    default:
                        messagePool.returnObject(message);
                        return;
                }

            case CancelOrder:
                Order orderToCancel = idToOrderMap.get(message.getOrderId());
                Message cancelMessage = messagePool.acquireObject();
                cancelMessage.setType(MessageType.CancelOrder);
                cancelMessage.setOrderId(orderToCancel.getClientId());
                orderToCancel.getLimit().getProcessor().getDistributorInboundQueue().add(cancelMessage);
                break;

            case CancelAllOrders:
                clientIdToOrderMap.get(message.getClientId()).forEach(o -> {
                    Message newMessage = messagePool.acquireObject();
                    newMessage.setType(MessageType.CancelOrder);
                    newMessage.setOrderId(o.getClientId());
                    o.getLimit().getProcessor().getDistributorInboundQueue().add(newMessage);
                });


        }

    }

    @Override
    public int newMarketOrder(CcyPair pair, long price) {
        return 0;
    }
}
