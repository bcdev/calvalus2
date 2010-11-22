package com.bc.calvalus.binning;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple map-backed bin store.
 */
class MyBinStore implements BinStore<MyBin> {
    Map<Integer, MyBin> binMap = new HashMap<Integer, MyBin>();

    @Override
    public MyBin getBin(int binIndex) {
        MyBin myBin = binMap.get(binIndex);
        if (myBin != null) {
            return myBin;
        }
        myBin = new MyBin(binIndex);
        putBin(myBin);
        return myBin;
    }

    @Override
    public void putBin(MyBin bin) {
        binMap.put(bin.getIndex(), bin);
    }
}
