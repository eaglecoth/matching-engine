package com.crypto.engine;

import com.crypto.data.*;
import com.crypto.feed.ObjectPool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * The OrderBookProcessor is an instance to represent and manage one side of a book for a particular currency pair
 * It holds Limits in a pseudo linked lists allowing for market orders to be matched in O(1) time relative to the
 * size of the book.
 */
public abstract class OrderBookProcessor {
    private final CcyPair pair;
    private final HashMap<Long, HashSet<Order>> clientToOrdersMap;
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
    protected final HashMap<Long, Order> idToOrderMap;

    public OrderBookProcessor(CcyPair pair, ObjectPool<Order> orderObjectPool, ObjectPool<Execution> executionObjectPool, ObjectPool<Message> messageObjectPool, ConcurrentLinkedQueue<Message> distributorInboundQueue, ConcurrentLinkedQueue<Execution> executionPublishQueue, AtomicLong orderCounter) {
        this.orderObjectPool = orderObjectPool;
        this.distributorInboundQueue = distributorInboundQueue;
        this.executionPublishQueue = executionPublishQueue;
        this.executionObjectPool = executionObjectPool;
        this.messageObjectPool = messageObjectPool;
        this.orderCounter = orderCounter;
        this.executions = new LinkedList<>();
        this.idToOrderMap = new HashMap<>();
        this.clientToOrdersMap = new HashMap<>();
        this.pair = pair;


        launchOrderBookProcessor(distributorInboundQueue);
    }

    private void launchOrderBookProcessor(ConcurrentLinkedQueue<Message> distributorInboundQueue) {
        Thread engineThread = new Thread(() -> {
            System.out.println("Order Book Processor on ccy: [" + pair + "] on side: [" + getSide() +"] started.");

            while (runningFlag) {
                Message message = distributorInboundQueue.poll();
                if (message != null) {
                    processMessage(message);
                }
            }
        });

        engineThread.start();
    }

    protected abstract Side getSide();
    protected abstract Side getOppositeSide();

    private void processMessage(Message message) {

        switch (message.getType()) {
            case CancelOrder:
                Order orderToCancel = idToOrderMap.remove(message.getOrderId());

                //If the cancel order is the last in its limit, we should remove the limit entirely from the book
                if (orderToCancel != null) {
                    Set<Order> clientOrders = clientToOrdersMap.get(message.getClientId());
                    clientOrders.remove(orderToCancel);
                    if(clientOrders.isEmpty()){
                        clientToOrdersMap.remove(message.getClientId());
                    }
                    if(orderToCancel.cancelOrder()) {
                        LimitLevel limitLevelToRemove = orderToCancel.getLimit();
                        orderBook.remove(limitLevelToRemove.getPrice());
                        if (limitLevelToRemove.removeLimitFromOrderbook()) {
                            topOfBook = null;
                        }
                    }
                    reportCancelAccepted(message.getOrderId());
                    orderObjectPool.returnObject(orderToCancel);
                    messageObjectPool.returnObject(message);
                }
                return;

            case CancelAllOrders:
                Set<Order> clientOrders = clientToOrdersMap.remove(message.getClientId());
                if(clientOrders == null || clientOrders.isEmpty()){
                   return;
                }else{
                    clientOrders.forEach(o -> {
                        if(o.cancelOrder()){
                            LimitLevel limitLevelToRemove = o.getLimit();
                            orderBook.remove(limitLevelToRemove.getPrice());
                            if (limitLevelToRemove.removeLimitFromOrderbook()) {
                                topOfBook = null;
                            }
                        };
                        reportCancelAccepted(o.getOrderId());
                        orderObjectPool.returnObject(o);
                    });
                }
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

    /**
     * Helper method to match market orders vs limit orders in the book.  The method will traverse and remove,
     * partially fill execution until the entire quantity of the market order is filled or the liquidity is entirely
     * dried up
     * @param message containing a market order to be filled against the limit orders in book
     */
    private void match(Message message) {

        Order insideBookOrder = topOfBook.peekInsideOfBook();
        long fillSize = message.getQuantity();

        if (insideBookOrder.getSize() > fillSize) {
            insideBookOrder.setSize(insideBookOrder.getSize() - fillSize);
            publishFill(message.getClientId(), fillSize, topOfBook, message.getPair(), getOppositeSide());
            publishFill(insideBookOrder.getClientId(), fillSize, topOfBook, message.getPair(), getSide());
        } else {

            insideBookOrder = topOfBook.pollInsideOfBook();

            long insideBookOrderSize = insideBookOrder.getSize();
            publishFill(message.getClientId(), insideBookOrderSize, topOfBook, message.getPair(), getOppositeSide());
            publishFill(insideBookOrder.getClientId(), insideBookOrderSize, topOfBook, message.getPair(), getSide());


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

    /**
     * Helper method to insert a limit order into its appropriate limit, and if such limit does not exist
     * then create that as well and place it correctly in the linked list of limit levels.
     * Inserts / lookup of exiting limits in O(log n) relative to number of price limit levels.
     * @param message containing a limit order which is to be inserted into the book.
     */
    private void insertOrderOnLimit(Message message) {
        long orderId = orderCounter.getAndIncrement();

        LimitLevel currentLimitLevel = orderBook.compute(message.getPrice(), (priceLevel, limit) -> {

            //If this is the first order of this price create the new limit book
            if (limit == null) {
                limit = new LimitLevel(priceLevel, executionObjectPool, this);

                //Unless this is the first order entirely in this book, traverse the list and insert the new limit
                //where it fits in.
                if (topOfBook != null) {
                    insertInChain(limit, topOfBook);
                } else {
                    topOfBook = limit;
                }
            }

            //Finally grab an order object form the pool and place it on the limit either acquired or created
            Order order = orderObjectPool.acquireObject();
            order.populate(orderId, message, limit);
            limit.addOrder(order);


            //Add the order to the set of its client orders and id to order map
            idToOrderMap.put(orderId, order);
            clientToOrdersMap.compute(message.getClientId(), (client,orderSet) ->{
                if(orderSet == null ){
                    orderSet = new HashSet<>();
                }
                orderSet.add(order);

                return orderSet;
            });

            return limit;
        });

        reevaluateTopOfBook(currentLimitLevel);
    }

    /**
     * Helper method to publish order fills
     * @param clientId filling client
     * @param size size of fill
     * @param limitLevel price
     * @param pair which currency pair
     * @param side which side
     */
    private void publishFill(long clientId, Long size, LimitLevel limitLevel, CcyPair pair, Side side) {
        Execution execution = executionObjectPool.acquireObject();
        execution.setClientId(clientId);
        execution.setQuantity(size);
        execution.setCcyPair(pair);
        execution.setPrice(limitLevel.getPrice());
        execution.setSide(side);
        executionPublishQueue.add(execution);
    }

    /**
     * Helper method to report acceptance of cancellation requests
     * @param orderId id of order which has been cancelled
     */
    private void reportCancelAccepted(long orderId) {
        Execution execution = executionObjectPool.acquireObject();
        execution.setType(ExecutionType.CancelAccepted);
        execution.setOrderId(orderId);
        executionPublishQueue.add(execution);
    }

    public ConcurrentLinkedQueue<Message> getDistributorInboundQueue() {
        return distributorInboundQueue;
    }

    public void shutdown() {
        System.out.println("Order Book Processor on ccy: [" + pair + "] on side: [" + getSide() +"] shutting down.");
        runningFlag = false;
    }

    abstract void insertInChain(LimitLevel newLimitLevel, LimitLevel currentLimitLevel);

    abstract void reevaluateTopOfBook(LimitLevel newLimitLevel);

    abstract LimitLevel getNextLevelLimit(LimitLevel limitLevelToExecute);


}
