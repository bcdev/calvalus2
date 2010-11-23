package com.bc.calvalus.b3;


public interface VariableContext {
    int getNumVariable(); // todo - rename getVariableCount 
    String getVariableName(int i);
    int getVariableIndex(String varName);
}
