package com.crypto.feed;

/**
 * Helper interface for pools to be able to create new objects
 * @param <T> type of objects returned from the instantiator
 */
public interface ObjectInstantiator<T> {

    public T newInstance();
}
