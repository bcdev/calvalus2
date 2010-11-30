package com.bc.calvalus.b3;

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
