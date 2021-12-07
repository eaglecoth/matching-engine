package com.crypto.feed;

import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * This is an infinitly growing memory pool which can be accessed in a non blocking fashion as it is implemented
 * with an unbounded compare-and-swap based queue.  The pool will only grow if it has run out of objects. Ideally
 * Users of the pool should return object once processing with them are completed.  This way references can be held,
 * propagated to the old generation and avoid being garbage collected.
 *
 * @param <T> type of object to be held in pool
 */
public class ObjectPool<T> {

    private final ObjectInstantiator<T> objectCreator;
    private final ConcurrentLinkedQueue<T> messagePool = new ConcurrentLinkedQueue<T>();

    public ObjectPool(ObjectInstantiator<T> objectCreator) {
        this.objectCreator = objectCreator;
    }

    public T acquireObject(){

        T instance = messagePool.poll();
        if(instance == null){
            instance =  objectCreator.newInstance();
        }
        return instance;
    }

    public void returnObject(T instance){
        messagePool.add(instance);
    }


    public long getSize(){
        return messagePool.size();
    }
}
