package com.bc.calvalus.binning;

/**
 * The binning context.
 *
 * @author Norman Fomferra
 */
public class BinningContext {

    private final BinningGrid binningGrid;
    private final BinManager binManager;
    private final int superSampling;

    public BinningContext(BinningGrid binningGrid, BinManager binManager) {
        this(binningGrid, binManager, 1);
    }

    public BinningContext(BinningGrid binningGrid, BinManager binManager, int superSampling) {
        this.binningGrid = binningGrid;
        this.binManager = binManager;
        this.superSampling = superSampling;
    }

    public VariableContext getVariableContext() {
        return getBinManager().getVariableContext();
    }

    public BinningGrid getBinningGrid() {
        return binningGrid;
    }

    public BinManager getBinManager() {
        return binManager;
    }

    public Integer getSuperSampling() {
        return superSampling;
    }
}
