package com.bc.calvalus.b3;

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
