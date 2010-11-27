package com.bc.calvalus.b3;

// todo - this is the variable context for all variables in 'product' that are referenced ...
// todo - ... by the given processing request. It therefore provided the list of actual...
// todo - ... raster data nodes to be read from 'product'.
// todo - It directly is associated with the 'ObservationImpl' used in L3Mapper. 

public class VariableContextImpl implements VariableContext {
    private final String[] varNames;

    public VariableContextImpl(String... varNames) {
        this.varNames = varNames.clone();
    }

    @Override
    public int getVariableCount() {
        return varNames.length;
    }

    @Override
    public String getVariableName(int i) {
        return varNames[i];
    }

    @Override
    public int getVariableIndex(String varName) {
        for (int i = 0; i < varNames.length; i++) {
            String name = varNames[i];
            if (name.equals(varName)) {
                return i;
            }
        }
        return -1;
    }
}
