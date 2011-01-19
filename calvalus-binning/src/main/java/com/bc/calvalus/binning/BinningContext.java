package com.bc.calvalus.binning;

/**
 * The binning context.
 *
 * @author Norman Fomferra
 */
public interface BinningContext {

    BinningGrid getBinningGrid();


    // todo - change to InputPropertyContext getInputPropertyContext()
    VariableContext getVariableContext();

    BinManager getBinManager();
}
