package com.bc.calvalus.binning;

public class BinnerContextImpl implements BinnerContext {
    private final BinningGrid binningGrid;
    private final VariableContext variableContext;
    private final BinManager binManager;

    public BinnerContextImpl(BinningGrid binningGrid, VariableContext variableContext, BinManager binManager) {
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
