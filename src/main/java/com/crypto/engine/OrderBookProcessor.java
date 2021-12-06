package com.crypto.engine;

import com.crypto.data.CcyPair;
import com.crypto.data.Message;
import com.crypto.data.Order;
import com.crypto.feed.ObjectPool;

import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * The OrderBookProcessor is an instance to represent and manage one side of a book for a particular currency pair
 * It holds Limits in a
 */
public abstract class OrderBookProcessor {
    protected ConcurrentLinkedQueue<Message> distributorInboundQueue;
    protected volatile boolean runningFlag = true;
    protected final TreeMap<Long, LimitLevel> orderBook = new TreeMap<>();
    protected final AtomicLong orderCounter;
    protected final ObjectPool<Order> orderObjectPool;
    protected final ObjectPool<Message> messageObjectPool;
    protected LimitLevel topOfBook;

    protected final LinkedList<Execution> executions;
    protected final ConcurrentLinkedQueue<Execution> executionPublishQueue;
    protected final ObjectPool<Execution> executionObjectPool;
    protected final ConcurrentHashMap<Long, Order> idToOrderMap;

    public OrderBookProcessor(CcyPair pair, ObjectPool<Order> orderObjectPool, ObjectPool<Execution> executionObjectPool, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Execution> executionPublishQueue, AtomicLong orderCounter, ConcurrentHashMap<Long, Order> idToOrderMap) {
        this.orderObjectPool = orderObjectPool;
        this.distributorInboundQueue = distributorInboundQueue;
        this.executionPublishQueue = executionPublishQueue;
        this.executionObjectPool = executionObjectPool;
        this.messageObjectPool = messageObjectPool;
        this.orderCounter = orderCounter;
        this.executions = new LinkedList<>();
        this.idToOrderMap = idToOrderMap;

        launchOrderBookProcessor(distributorInboundQueue);
    }

    private void launchOrderBookProcessor(ConcurrentLinkedQueue<Message> distributorInboundQueue) {
        new Thread(() -> {
            System.out.println("Order Book Distributor Started");

            while (runningFlag) {
                Message message = distributorInboundQueue.poll();
                if (message != null) {
                    processMessage(message);
                }
            }
        });
    }

    private void processMessage(Message message) {

        switch (message.getType()) {
            case CancelOrder:
                Order orderToCancel = idToOrderMap.remove(message.getOrderId());
                if (orderToCancel != null && orderToCancel.cancelOrder()) {
                    LimitLevel limitLevelToRemove = orderToCancel.getLimit();
                    orderBook.remove(limitLevelToRemove.getPrice());
                    if (limitLevelToRemove.removeLimitFromOrderbook()) {
                        topOfBook = null;
                    }
                }
                orderObjectPool.returnObject(orderToCancel);
                messageObjectPool.returnObject(message);
                return;

            case NewLimitOrder:
                insertOrderOnLimit(message);
                messageObjectPool.returnObject(message);
                return;

            case NewMarketOrder:
                if (topOfBook == null) {
                    System.out.println("There are no orders in the book to execute. Droppign order");
                    messageObjectPool.returnObject(message);
                    return;
                }
                match(message);
                messageObjectPool.returnObject(message);
        }

    }

    private void match(Message message) {

        Order insideBookOrder = topOfBook.peekInsideOfBook();
        long fillSize = message.getQuantity();

        if (insideBookOrder.getSize() > fillSize) {
            insideBookOrder.setSize(insideBookOrder.getSize() - fillSize);
            publishExecution(message.getClientId(), fillSize, topOfBook, message.getPair());
            publishExecution(insideBookOrder.getClientId(), fillSize, topOfBook, message.getPair());
        } else {

            insideBookOrder = topOfBook.pollInsideOfBook();

            long insideBookOrderSize = insideBookOrder.getSize();
            publishExecution(insideBookOrder.getClientId(), insideBookOrderSize, topOfBook, message.getPair());
            publishExecution(message.getClientId(), insideBookOrderSize, topOfBook, message.getPair());

            if (topOfBook.isEmpty()) {
                LimitLevel newTopOfBook = getNextLevelLimit(topOfBook);
                topOfBook.removeLimitFromOrderbook();
                topOfBook = newTopOfBook;

                if(topOfBook == null){
                    System.out.println("Orderbook has dried up. No more liqiuidity to execute");
                    return;
                }
            }

            if (fillSize != insideBookOrderSize) {
                message.setQuantity(fillSize - insideBookOrderSize);
                match(message);
            }
        }
    }

    private void insertOrderOnLimit(Message message) {
        long orderId = orderCounter.getAndIncrement();

        LimitLevel currentLimitLevel = orderBook.compute(message.getPrice(), (priceLevel, limit) -> {

            //If this is the first order of this price create the new limit book
            if (limit == null) {
                LimitLevel newLimitLevel = new LimitLevel(priceLevel, executionObjectPool, this);

                //Unless this is the first order entirely in this book, traverse the list and insert the new limit
                //where it fits in.
                if (topOfBook != null) {
                    insertInChain(newLimitLevel, topOfBook);
                } else {
                    topOfBook = newLimitLevel;
                }
            }

            //Finally grab an order object form the pool and place it on the limit either acquired or created
            Order order = orderObjectPool.acquireObject();
            order.populate(orderId, message, limit);
            limit.addOrder(order);

            return limit;
        });

        reevaluateTopOfBook(currentLimitLevel);
    }

    private void publishExecution(long clientId, Long size, LimitLevel limitLevel, CcyPair pair) {
        Execution execution = executionObjectPool.acquireObject();
        execution.setClientId(clientId);
        execution.setQuantity(size);
        execution.setCcyPair(pair);
        execution.setPrice(limitLevel.getPrice());
        executionPublishQueue.add(execution);
    }

    public void shutdown() {
        runningFlag = false;
    }

    abstract void insertInChain(LimitLevel newLimitLevel, LimitLevel currentLimitLevel);

    abstract void reevaluateTopOfBook(LimitLevel newLimitLevel);

    abstract LimitLevel getNextLevelLimit(LimitLevel limitLevelToExecute);

    public ConcurrentLinkedQueue<Message> getDistributorInboundQueue() {
        return distributorInboundQueue;
    }
}
