package com.bc.calvalus.binning;

/**
 * The binning context.
 *
 * @author Norman Fomferra
 */
public class BinningContext {

    private final BinningGrid binningGrid;
    private final VariableContext variableContext;
    private final BinManager binManager;

    public BinningContext(BinningGrid binningGrid,
                          VariableContext variableContext,
                          BinManager binManager) {
        this.binningGrid = binningGrid;
        this.variableContext = variableContext;
        this.binManager = binManager;
    }

    public BinningGrid getBinningGrid() {
        return binningGrid;
    }

    public VariableContext getVariableContext() {
        return variableContext;
    }

    public BinManager getBinManager() {
        return binManager;
    }

}
