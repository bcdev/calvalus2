package com.bc.calvalus.b3;

/**
* todo - add api doc
*
* @author Norman Fomferra
*/
class MyVariableContext implements VariableContext {
    private String[] varNames;

    MyVariableContext(String ... varNames) {
        this.varNames = varNames;
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
            if (varName.equals(varNames[i])) {
                return i;
            }
        }
        return -1;
    }
}
