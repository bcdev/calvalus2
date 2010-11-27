package com.bc.calvalus.b3;

public class BinningContextImpl implements BinningContext {
    private final BinningGrid binningGrid;
    private final VariableContext variableContext;
    private final BinManager binManager;

    public BinningContextImpl(BinningGrid binningGrid, VariableContext variableContext, BinManager binManager) {
        this.binningGrid = binningGrid;
        this.variableContext = variableContext;
        this.binManager = binManager;
    }

    @Override
    public BinningGrid getBinningGrid() {
        return binningGrid;
    }

    @Override
    public VariableContext getVariableContext() {
        return variableContext;
    }

    @Override
    public BinManager getBinManager() {
        return binManager;
    }

}
