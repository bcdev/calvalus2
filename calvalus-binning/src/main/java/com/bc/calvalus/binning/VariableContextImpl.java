package com.bc.calvalus.binning;

import java.util.ArrayList;

/**
 *  This is the variable context for all variables referenced
 *  from a given L3 processing request. It therefore provides the list of actual
 *  raster data nodes to be read from a given input data product.
 *  <p/>
 *  The i=0...n-1 measurements in an {@link Observation} directly correspond
 *  to the i=0...n-1 variables provided by {@code VariableContext}.
 *
 *  @author Norman
 */
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
