package com.crypto.engine;

import com.crypto.data.Message;
import com.crypto.data.MessageType;
import com.crypto.data.Order;
import com.crypto.feed.ObjectPool;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Class responsible for unpacking instructions and sending them for processing to the correct threads.
 * One thread running for each Currency pairs side of book.
 * Non blocking thread communication is provided via ConcurrentLinkedQueues.
 */
public class OrderBookDistributor {

    private volatile boolean runningFlag = true;
    private final ConcurrentHashMap<Long, List<Order>> clientIdToOrderMap;
    private final ConcurrentLinkedQueue<Message> incomingMessageQueue;
    private final ConcurrentLinkedQueue<Message> btcUsdOfferBookQueue;
    private final ConcurrentLinkedQueue<Message> btcUsdBidBookQueue;
    private final ConcurrentLinkedQueue<Message> ethUsdOfferBookQueue;
    private final ConcurrentLinkedQueue<Message> ethUsdBidBookQueue;
    private final ObjectPool<Message> messagePool;

    public OrderBookDistributor(ConcurrentLinkedQueue<Message> inboundQueue, List<ConcurrentLinkedQueue<Message>> engineQueues, ConcurrentHashMap<Long, List<Order>> clientIdToOrderMap, ObjectPool<Message> messagePool) {

        this.messagePool = messagePool;
        this.clientIdToOrderMap = clientIdToOrderMap;

        incomingMessageQueue = inboundQueue;
        btcUsdOfferBookQueue = engineQueues.get(0);
        btcUsdBidBookQueue = engineQueues.get(1);
        ethUsdOfferBookQueue = engineQueues.get(2);
        ethUsdBidBookQueue = engineQueues.get(3);

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

    /**
     * Helper method to decide which queue to send a particular request to.
     * @param message
     */
    private void processMessage(Message message) {

        switch (message.getType()) {

            case NewMarketOrder:
                switch (message.getSide()) {
                    case Bid:
                        switch (message.getPair()) {
                            case ETHUSD:
                                ethUsdOfferBookQueue.add(message);
                                return;
                            case BTCUSD:
                                btcUsdOfferBookQueue.add(message);
                                return;
                        }

                    case Offer:
                        switch (message.getPair()) {
                            case ETHUSD:
                                ethUsdBidBookQueue.add(message);
                                return;
                            case BTCUSD:
                                btcUsdBidBookQueue.add(message);
                                return;
                        }

                    default:
                        System.out.println("Unexpected Message Type which is not handled: " + message.getType());
                        messagePool.returnObject(message);
                        return;
                }

            case NewLimitOrder:
                switch (message.getSide()) {
                    case Bid:
                        switch (message.getPair()) {
                            case ETHUSD:
                                ethUsdBidBookQueue.add(message);
                                return;
                            case BTCUSD:
                                btcUsdBidBookQueue.add(message);
                                return;
                        }

                    case Offer:
                        switch (message.getPair()) {
                            case ETHUSD:
                                ethUsdOfferBookQueue.add(message);
                                return;
                            case BTCUSD:
                                btcUsdOfferBookQueue.add(message);
                                return;
                        }

                    default:
                        System.out.println("Unexpected Message Type which is not handled: " + message.getType());
                        messagePool.returnObject(message);
                        return;
                }

            case CancelOrder:
            case CancelAllOrders:
                btcUsdBidBookQueue.add(message);
                btcUsdOfferBookQueue.add(message);
                ethUsdBidBookQueue.add(message);
                ethUsdOfferBookQueue.add(message);
                break;
        }
    }

    public void shutdown() {
        System.out.println("Shuttingdown OrderBook Distributor");
        runningFlag = false;
    }
}
