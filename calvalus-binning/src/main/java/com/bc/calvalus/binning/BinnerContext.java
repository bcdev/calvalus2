package com.bc.calvalus.binning;

/**
 * The binning context.
 *
 * @author Norman Fomferra
 */
public interface BinnerContext {

    BinningGrid getBinningGrid();

    VariableContext getVariableContext();

    BinManager getBinManager();
}
