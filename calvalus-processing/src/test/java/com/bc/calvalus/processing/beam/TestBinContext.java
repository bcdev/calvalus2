package com.bc.calvalus.processing.beam;

import org.esa.snap.binning.BinContext;

import java.util.HashMap;

public class TestBinContext implements BinContext {

    private HashMap<String, Object> map = new HashMap<String, Object>();

    @Override
    public long getIndex() {
        return 0;
    }

    @Override
    public <T> T get(String name) {
        return (T) map.get(name);
    }

    @Override
    public void put(String name, Object value) {
        map.put(name, value);
    }

}
