package com.bc.calvalus.b3;

public class BinningContextImpl implements BinningContext {
    private final BinningGrid binningGrid;
    private final BinManager binManager;

    public BinningContextImpl(BinningGrid binningGrid, BinManager binManager) {
        this.binningGrid = binningGrid;
        this.binManager = binManager;
    }

    @Override
    public BinningGrid getBinningGrid() {
        return binningGrid;
    }

    @Override
    public BinManager getBinManager() {
        return binManager;
    }

}
