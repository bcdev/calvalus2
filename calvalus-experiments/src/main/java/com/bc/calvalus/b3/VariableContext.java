package com.bc.calvalus.b3;


public interface VariableContext {
    int getVariableCount();
    String getVariableName(int i);
    String getVariableExpr(int i);
    int getVariableIndex(String varName);

    String getMaskExpr();
}
