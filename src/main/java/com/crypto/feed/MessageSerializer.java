package com.crypto.feed;

public interface MessageSerializer {

    boolean onMessage(String message);

}
