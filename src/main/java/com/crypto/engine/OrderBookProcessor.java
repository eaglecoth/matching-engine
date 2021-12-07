package com.crypto.engine;

import com.crypto.data.*;
import com.crypto.feed.ObjectPool;

import java.util.*;
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
    private final ObjectPool<LimitLevel> limitObjectPool;
    protected volatile LimitLevel topOfBook;

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
        this.limitObjectPool = new ObjectPool<LimitLevel>(LimitLevel::new);

        launchOrderBookProcessor(distributorInboundQueue);
    }

    /**
     * Main processing method.  Incoming messages are categorised by type and processed accordingly. All processing
     * within the book itself happens synchronously.
     *
     * @param message message to be processed by the orderbook
     */
    private void processMessage(Message message) {

        switch (message.getType()) {
            case CancelOrder:
                Order orderToCancel = idToOrderMap.remove(message.getOrderId());

                //If the cancel order is the last in its limit, we should remove the limit entirely from the book
                if (orderToCancel != null) {
                    Set<Order> clientOrders = clientToOrdersMap.get(orderToCancel.getClientId());
                    clientOrders.remove(orderToCancel);
                    if(clientOrders.isEmpty()){
                        clientToOrdersMap.remove(message.getClientId());
                    }
                    //If the order was the last on the limit, we should remove the limit.
                    if(orderToCancel.cancelOrder()) {
                        LimitLevel limitLevelToRemove = orderToCancel.getLimit();
                        orderBook.remove(limitLevelToRemove.getPrice());
                        if (limitLevelToRemove.removeLimitFromOrderbook()) {
                            topOfBook = null;
                        }
                        limitObjectPool.returnObject(limitLevelToRemove);
                    }
                    reportCancelAccepted(orderToCancel);
                }
                messageObjectPool.returnObject(message);
                return;

            case CancelAllOrders:
                Set<Order> clientOrders = clientToOrdersMap.remove(message.getClientId());

                if(clientOrders != null && !clientOrders.isEmpty()){
                    clientOrders.forEach(o -> {
                        //Cancel order is last on particular price, remove the limit level
                        if(o.cancelOrder()){
                            LimitLevel limitLevelToRemove = o.getLimit();
                            orderBook.remove(limitLevelToRemove.getPrice());
                            if (limitLevelToRemove.removeLimitFromOrderbook()) {
                                topOfBook = null;
                            }
                            limitObjectPool.returnObject(limitLevelToRemove);
                        };
                        reportCancelAccepted(o);
                    });
                }
                messageObjectPool.returnObject(message);
                return;

            case NewLimitOrder:
                insertOrderOnLimit(message);
                messageObjectPool.returnObject(message);
                return;

            case NewMarketOrder:
                if (topOfBook == null) {
                    System.out.println("There are no orders in the book to execute. Rejecting Order");
                    sendReject(message);
                    return;
                }
                match(message);
                messageObjectPool.returnObject(message);
        }

    }

    private void sendReject(Message message) {
        Execution execution = executionObjectPool.acquireObject();
        execution.setType(ExecutionType.Reject);
        execution.setClientId(message.getClientId());
        execution.setClientOrderId(message.getClientOrderId());
        execution.setQuantity(message.getQuantity());
        executionPublishQueue.add(execution);
        messageObjectPool.returnObject(message);
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

        //Full fill on matching order direct -- The market order is smaller than the first order top of book
        if (insideBookOrder.getSize() > fillSize) {
            insideBookOrder.setSize(insideBookOrder.getSize() - fillSize);
            publishFill(message.getClientId(), fillSize, topOfBook, message.getPair(), getOppositeSide(), ExecutionType.Fill);
            publishFill(insideBookOrder.getClientId(), fillSize, topOfBook, message.getPair(), getSide(), ExecutionType.PartialFill);
            message.setQuantity(0);
        } else {

            insideBookOrder = topOfBook.pollInsideOfBook();

            long insideBookOrderSize = insideBookOrder.getSize();
            boolean marketGreaterThanLimitOrder = fillSize != insideBookOrderSize;
            publishFill(message.getClientId(), insideBookOrderSize, topOfBook, message.getPair(), getOppositeSide(), marketGreaterThanLimitOrder ? ExecutionType.PartialFill: ExecutionType.Fill );
            publishFill(insideBookOrder.getClientId(), insideBookOrderSize, topOfBook, message.getPair(), getSide(), ExecutionType.Fill);

            message.setQuantity(fillSize - insideBookOrderSize);

            if (topOfBook.isEmpty()) {
                LimitLevel newTopOfBook = getNextLevelLimit(topOfBook);
                if(topOfBook.removeLimitFromOrderbook()){
                    LimitLevel oldTopOfBook = topOfBook;
                    topOfBook = newTopOfBook;
                    limitObjectPool.returnObject(oldTopOfBook);
                }else{
                    topOfBook = newTopOfBook;
                }

                if(topOfBook == null && message.getQuantity() != 0){
                    System.out.println("Orderbook has dried up. No more liqiuidity to execute. Rejecting remainder");
                    sendReject(message);
                    return;
                }
            }

            if (message.getQuantity() >0) {
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
            if(limit == null) {
                limit = addNewPriceLevelToBook(priceLevel);
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

            reportOrderAccepted(order);
            return limit;
        });

        reevaluateTopOfBook(currentLimitLevel);
    }

    private void reportOrderAccepted(Order order) {
        Execution execution = executionObjectPool.acquireObject();
        execution.setType(ExecutionType.OrderAccepted);
        execution.setClientId(order.getClientId());
        execution.setPair(order.getLimit().getProcessor().getPair());
        execution.setClientOrderId(order.getClientOrderId());
        execution.setOrderId(order.getOrderId());
        execution.setPrice(order.getLimit().getPrice());
        execution.setQuantity(order.getSize());
        execution.setSide(getSide());
        executionPublishQueue.add(execution);
    }

    /**
     * Helper method to aqcuire a new object form pool and insert in to chain of price levels
     * @param priceLevel the price for which no current orders exist
     * @return  the newly added price limit
     */
    private LimitLevel addNewPriceLevelToBook(Long priceLevel) {
        LimitLevel limit = limitObjectPool.acquireObject();
        limit.populate(priceLevel, executionObjectPool, this);
        //Unless this is the first order entirely in this book, traverse the list and insert the new limit
        //where it fits in.
        if (topOfBook != null) {
            insertInChain(limit, topOfBook);
        } else {
            topOfBook = limit;
        }
        return limit;
    }

    /**
     * Helper method to publish order fills
     * @param clientId filling client
     * @param size size of fill
     * @param limitLevel price
     * @param pair which currency pair
     * @param side which side
     */
    private void publishFill(long clientId, Long size, LimitLevel limitLevel, CcyPair pair, Side side, ExecutionType execType) {
        Execution execution = executionObjectPool.acquireObject();
        execution.setClientId(clientId);
        execution.setQuantity(size);
        execution.setCcyPair(pair);
        execution.setPrice(limitLevel.getPrice());
        execution.setSide(side);
        execution.setType(execType);
        executionPublishQueue.add(execution);
    }

    /**
     * Helper method to report acceptance of cancellation requests
     * @param order to report cancelled
     */
    private void reportCancelAccepted(Order order) {
        Execution execution = executionObjectPool.acquireObject();
        execution.setType(ExecutionType.CancelAccepted);
        execution.setOrderId(order.getOrderId());
        execution.setClientOrderId(order.getClientOrderId());
        execution.setClientId(order.getClientId());
        executionPublishQueue.add(execution);
        orderObjectPool.returnObject(order);
    }

    public ConcurrentLinkedQueue<Message> getDistributorInboundQueue() {
        return distributorInboundQueue;
    }

    public CcyPair getPair() {
        return pair;
    }

    public void shutdown() {
        System.out.println("Order Book Processor on ccy: [" + pair + "] on side: [" + getSide() +"] shutting down.");
        runningFlag = false;
    }

    /**
     * Helper method to launch the processor in its own thread.
     * @param distributorInboundQueue Queue for which to poll for incoming orders / cancellations
     */
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

    //Methods to be implemented depending on side

    abstract void insertInChain(LimitLevel newLimitLevel, LimitLevel currentLimitLevel);

    abstract void reevaluateTopOfBook(LimitLevel newLimitLevel);

    abstract LimitLevel getNextLevelLimit(LimitLevel limitLevelToExecute);

    protected abstract Side getSide();

    protected abstract Side getOppositeSide();



}
