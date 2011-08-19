/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.l3;


import com.bc.calvalus.binning.*;
import com.bc.calvalus.processing.xml.XmlBinding;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.PropertyContainer;
import org.esa.beam.framework.gpf.annotations.Parameter;

@SuppressWarnings({"UnusedDeclaration"})
public class L3Config implements XmlConvertible {

    /**
     * Number of rows in the binning grid.
     */
    @Parameter
    int numRows;

    /**
     * The number of pixels used for super-sampling an input pixel into sub-pixel.
     */
    @Parameter
    Integer superSampling;

    /**
     * The band maths expression used to filter input pixels.
     */
    @Parameter
    String maskExpr;

    /**
     * List of variables. A variable will generate a {@link org.esa.beam.framework.datamodel.VirtualBand VirtualBand}
     * in the input data product to be binned, so that it can be used for binning.
     */
    @Parameter(itemAlias = "variable")
    VariableConfiguration[] variables;

    /**
     * List of aggregators. Aggregators generate the bands in the binned output products.
     */
    @Parameter(itemAlias = "aggregator")
    AggregatorConfiguration[] aggregators;

    public static L3Config fromXml(String xml) {
        return new XmlBinding().convertXmlToObject(xml, new L3Config());
    }

    @Override
    public String toXml() {
        return new XmlBinding().convertObjectToXml(this);
    }


    public int getNumRows() {
        return numRows;
    }

    public Integer getSuperSampling() {
        return superSampling;
    }

    public String getMaskExpr() {
        return maskExpr;
    }

    public VariableConfiguration[] getVariables() {
        return variables;
    }

    public AggregatorConfiguration[] getAggregators() {
        return aggregators;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public void setSuperSampling(Integer superSampling) {
        this.superSampling = superSampling;
    }

    public void setMaskExpr(String maskExpr) {
        this.maskExpr = maskExpr;
    }

    public void setVariables(VariableConfiguration... variables) {
        this.variables = variables;
    }

    public void setAggregators(AggregatorConfiguration... aggregators) {
        this.aggregators = aggregators;
    }

    public float[] getSuperSamplingSteps() {
        if (superSampling == null || superSampling < 1) {
            return new float[]{0.5f};
        } else {
            float[] samplingStep = new float[superSampling];
            for (int i = 0; i < samplingStep.length; i++) {
                samplingStep[i] = (i * 2 + 1f) / (2f * superSampling);
            }
            return samplingStep;
        }
    }

    public BinningContext getBinningContext() {
        VariableContext varCtx = getVariableContext();
        return new BinningContextImpl(getBinningGrid(),
                                      varCtx,
                                      getBinManager(varCtx));
    }

    public BinningGrid getBinningGrid() {
        if (numRows == 0) {
            numRows = IsinBinningGrid.DEFAULT_NUM_ROWS;
        }
        return new IsinBinningGrid(numRows);
    }

    private BinManager getBinManager(VariableContext varCtx) {
        Aggregator[] aggs = new Aggregator[aggregators.length];
        for (int i = 0; i < aggs.length; i++) {
            AggregatorConfiguration aggregatorConfiguration = aggregators[i];
            AggregatorDescriptor descriptor = AggregatorDescriptorRegistry.getInstance().getAggregatorDescriptor(aggregatorConfiguration.type);
            if (descriptor != null) {
                aggs[i] = descriptor.createAggregator(varCtx, PropertyContainer.createObjectBacked(aggregatorConfiguration));
            } else {
                throw new IllegalArgumentException("Unknown aggregator type: " + aggregatorConfiguration.type);
            }
        }
        return new BinManagerImpl(aggs);
    }

    public VariableContext getVariableContext() {
        VariableContextImpl variableContext = new VariableContextImpl();
        if (maskExpr == null) {
            maskExpr = "";
        }
        variableContext.setMaskExpr(maskExpr);

        // define declared variables
        //
        if (variables != null) {
            for (VariableConfiguration variable : variables) {
                variableContext.defineVariable(variable.name, variable.expr);
            }
        }

        // define variables of all aggregators
        //
        if (aggregators != null) {
            for (AggregatorConfiguration aggregator : aggregators) {
                String varName = aggregator.varName;
                if (varName != null) {
                    variableContext.defineVariable(varName);
                } else {
                    String[] varNames = aggregator.varNames;
                    if (varNames != null) {
                        for (String varName1 : varNames) {
                            variableContext.defineVariable(varName1);
                        }
                    }
                }
            }
        }
        return variableContext;
    }

    public static class VariableConfiguration {
        String name;

        String expr;

        public VariableConfiguration() {
        }

        public VariableConfiguration(String name, String expr) {
            this.name = name;
            this.expr = expr;
        }

        public String getName() {
            return name;
        }

        public String getExpr() {
            return expr;
        }
    }

    public static class AggregatorConfiguration {
        String type;

        String varName;

        String[] varNames;

        Integer percentage;

        Double weightCoeff;

        Float fillValue;

        public AggregatorConfiguration() {
        }

        public AggregatorConfiguration(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public String getVarName() {
            return varName;
        }

        public String[] getVarNames() {
            return varNames;
        }

        public Integer getPercentage() {
            return percentage;
        }

        public Double getWeightCoeff() {
            return weightCoeff;
        }

        public Float getFillValue() {
            return fillValue;

        }

        public void setType(String type) {
            this.type = type;
        }

        public void setVarName(String varName) {
            this.varName = varName;
        }

        public void setVarNames(String[] varNames) {
            this.varNames = varNames;
        }

        public void setPercentage(Integer percentage) {
            this.percentage = percentage;
        }

        public void setWeightCoeff(Double weightCoeff) {
            this.weightCoeff = weightCoeff;
        }

        public void setFillValue(Float fillValue) {
            this.fillValue = fillValue;
        }
    }

}
