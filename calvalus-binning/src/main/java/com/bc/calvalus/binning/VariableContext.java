package com.bc.calvalus.binning;

// todo - rename to InputPropertyContext
public interface VariableContext {
    int getVariableCount();
    String getVariableName(int i);
    String getVariableExpr(int i);
    int getVariableIndex(String varName);

    String getMaskExpr();
}
