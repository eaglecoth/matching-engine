package com.crypto.feed;

import java.util.concurrent.ConcurrentLinkedQueue;

public class ObjectPool<T> {

    private final ObjectInstantiator<T> objectCreator;
    private ConcurrentLinkedQueue<T> messagePool = new ConcurrentLinkedQueue<T>();

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
