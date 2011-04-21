package com.bc.calvalus.binning;

public interface VariableContext {
    int getVariableCount();
    String getVariableName(int varIndex);
    String getVariableExpr(int varIndex);
    int getVariableIndex(String varName);
    String getMaskExpr();
}
