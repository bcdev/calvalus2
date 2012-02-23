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
    private final int superSampling;

    public BinningContext(BinningGrid binningGrid, VariableContext variableContext, BinManager binManager) {
        this(binningGrid, variableContext, binManager, 1);
    }

    public BinningContext(BinningGrid binningGrid,
                          VariableContext variableContext,
                          BinManager binManager, int superSampling) {
        this.binningGrid = binningGrid;
        this.variableContext = variableContext;
        this.binManager = binManager;
        this.superSampling = superSampling;
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

    public Integer getSuperSampling() {
        return superSampling;
    }
}
