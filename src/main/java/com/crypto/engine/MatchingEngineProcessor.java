package com.crypto.engine;

import com.crypto.data.CcyPair;

public interface MatchingEngineProcessor {


    public int newMarketOrder(CcyPair pair, long price);

}
