package com.bc.calvalus.binning;

/**
 * The binning context.
 *
 * @author Norman Fomferra
 */
public interface BinningContext {

    BinningGrid getBinningGrid();

    VariableContext getVariableContext();

    BinManager getBinManager();
}
