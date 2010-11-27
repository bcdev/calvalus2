package com.bc.calvalus.b3;

import java.util.ArrayList;

// todo - this is the variable context for all variables in 'product' that are referenced ...
// todo - ... by the given processing request. It therefore provided the list of actual...
// todo - ... raster data nodes to be read from 'product'.
// todo - It directly is associated with the 'ObservationImpl' used in L3Mapper. 

public class VariableContextImpl implements VariableContext {
    private final ArrayList<String> names;
    private final ArrayList<String> exprs;
    private String maskExpr;

    public VariableContextImpl() {
        this.names = new ArrayList<String>();
        this.exprs = new ArrayList<String>();
    }

    public void defineVariable(String name) {
        defineVariable(name, null);
    }

    public void defineVariable(String name, String expr) {
        final int index = names.indexOf(name);
        if (index >= 0) {
            if (expr != null) {
                exprs.set(index, expr);
            }
        } else {
            names.add(name);
            exprs.add(expr);
        }
    }

    @Override
    public String getMaskExpr() {
        return maskExpr;
    }

    public void setMaskExpr(String maskExpr) {
        this.maskExpr = maskExpr;
    }

    @Override
    public int getVariableCount() {
        return names.size();
    }

    @Override
    public String getVariableName(int i) {
        return names.get(i);
    }

    @Override
    public String getVariableExpr(int i) {
        return exprs.get(i);
    }

    @Override
    public int getVariableIndex(String varName) {
        return names.indexOf(varName);
    }
}
