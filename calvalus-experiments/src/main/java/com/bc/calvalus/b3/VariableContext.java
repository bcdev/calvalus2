package com.bc.calvalus.b3;

// todo - rename to InputPropertyContext
public interface VariableContext {
    int getVariableCount();
    String getVariableName(int i);
    String getVariableExpr(int i);
    int getVariableIndex(String varName);

    String getMaskExpr();
}
