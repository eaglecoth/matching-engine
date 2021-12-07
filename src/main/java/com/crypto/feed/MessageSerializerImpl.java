package com.crypto.feed;

import com.crypto.data.CcyPair;
import com.crypto.data.Message;
import com.crypto.data.MessageType;
import com.crypto.data.Side;

import java.util.concurrent.ConcurrentLinkedQueue;

import static com.crypto.data.Constants.*;


/**
 * Not sure if this was really necessary.  The engine needs some serializer mechanism. This one is stupid.
 */
public class MessageSerializerImpl implements MessageSerializer {

    private final ConcurrentLinkedQueue<Message> engineMessageQueue;
    private final ObjectPool<Message> messageObjectPool;
    private final String stringDelimiter;
    private long offerRetryCount;
    private long sleepTimeMillis;

    public MessageSerializerImpl(ConcurrentLinkedQueue<Message> messageQueue, ObjectPool objectPool, long retryCount, long waitTimeMillis, String delimiter) {

        engineMessageQueue = messageQueue;
        messageObjectPool = objectPool;
        offerRetryCount = retryCount;
        sleepTimeMillis = waitTimeMillis;
        stringDelimiter = delimiter;
    }

    /**
     *
     * @param messageString instruction to send to matching engine
     * @return true if message was sucessfully submitted, else false
     * @throws InterruptedException thrown if thread is interrupted while sleeping in hope of engine to recover
     */
    public boolean onMessage(String messageString) {

        Message message = deserialize(messageString);
        if(message == null){
            return false;
        }

        if(!engineMessageQueue.offer(message)) {
            long currentRetryCount = offerRetryCount;

            while(!engineMessageQueue.offer(message) && currentRetryCount > 0){
                System.out.println("ERROR: Queue is full.  What do I do now? Just wait?");
                try {
                    Thread.sleep(sleepTimeMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return false;
                }
                currentRetryCount -=1;
            }
        }

        return true;
    }


    private Message deserialize(String msgToDeSerialize){

        String[] messageString = msgToDeSerialize.split(stringDelimiter);
        Message message = messageObjectPool.acquireObject();

        if(messageString[0] == null){
            return null;
        }

        switch(messageString[0]){

            case NEW_MARKET_ORDER:
                message.setType(MessageType.NewMarketOrder);
                message.setClientId(Long.valueOf(messageString[1]));
                message.setClientOrderId(Long.valueOf(messageString[2]));
                CcyPair pair = parseCcyPair(messageString[3]);
                if(pair == null){
                    messageObjectPool.returnObject(message);
                    return null;
                }
                message.setPair(pair);

                Side side = parseSide(messageString[4]);
                if(side == null){
                    messageObjectPool.returnObject(message);
                    return null;
                }
                message.setSide(side);
                return message;

            case NEW_LIMIT_ORDER:
                message.setType(MessageType.NewLimitOrder);
                message.setClientId(Long.valueOf(messageString[1]));
                message.setClientOrderId(Long.valueOf(messageString[2]));
                pair = parseCcyPair(messageString[3]);
                if(pair == null){
                    messageObjectPool.returnObject(message);
                    return null;
                }
                message.setPair(pair);

                side = parseSide(messageString[4]);
                if(side == null){
                    messageObjectPool.returnObject(message);
                    return null;
                }
                message.setSide(side);
                message.setLimit(Long.valueOf(messageString[5]));
                return message;

            case CANCEL_ORDER:
                message.setType(MessageType.CancelOrder);
                message.setClientId(Long.valueOf(messageString[1]));
                message.setOrderId(Long.valueOf(messageString[2]));
                return message;

            case CANCEL_ALL:
                message.setType(MessageType.CancelAllOrders);
                message.setClientId(Long.valueOf(messageString[1]));
                return message;

            default:
                System.out.println("What happened here?  I don't handle messages of type " + messageString[0]);
                messageObjectPool.returnObject(message);
        }

        return null;
    }

    private Side parseSide(String sideString) {

        if(sideString == null){
            return null;
        }
        switch (sideString){

            case BID:
                return Side.Bid;

            case OFFER:
                return Side.Offer;

            default:
                System.out.println("I can't interpret side " + sideString);
                return null;
        }

    }

    private CcyPair parseCcyPair(String ccyString){

        if(ccyString == null){
            return null;
        }
        switch (ccyString){

            case BTCUSD:
                return CcyPair.BTCUSD;

            case ETHUSD:
                return CcyPair.ETHUSD;

            default:
                System.out.println("I don't offer currency pair " + ccyString);
                return null;
        }

    }
}
