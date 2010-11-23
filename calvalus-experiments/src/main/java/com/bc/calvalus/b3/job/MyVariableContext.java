package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.VariableContext;

// todo - this is the variable context for all variables in 'product' that are referenced ...
// todo - ... by the given processing request. It therefore provided the list of actual...
// todo - ... raster data nodes to be read from 'product'.
// todo - It directly is associated with the 'ObservationImpl' used in L3Mapper. 

class MyVariableContext implements VariableContext {
    @Override
    public int getNumVariable() {
        return 1;
    }

    @Override
    public String getVariableName(int i) {
        return "ndvi";
    }

    @Override
    public int getVariableIndex(String varName) {
        return 0;
    }
}
