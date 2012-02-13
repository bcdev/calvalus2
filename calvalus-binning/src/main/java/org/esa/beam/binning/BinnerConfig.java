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

package org.esa.beam.binning;


import com.bc.calvalus.binning.*;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.PropertyContainer;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.framework.gpf.annotations.ParameterBlockConverter;

import java.util.Arrays;

/**
 * Configuration for the binning.
 *
 * @author Norman Fomferra
 * @author Marco Zühlke
 */
@SuppressWarnings({"UnusedDeclaration"})
public class BinnerConfig {

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
    @Parameter(alias = "variables", itemAlias = "variable")
    VariableConfiguration[] variableConfigurations;

    /**
     * List of aggregators. Aggregators generate the bands in the binned output products.
     */
    @Parameter(alias = "aggregators", itemAlias = "aggregator")
    AggregatorConfiguration[] aggregatorConfigurations;

    public int getNumRows() {
        return numRows;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public String getMaskExpr() {
        return maskExpr;
    }

    public void setMaskExpr(String maskExpr) {
        this.maskExpr = maskExpr;
    }

     public Integer getSuperSampling() {
        return superSampling;
    }

    public void setSuperSampling(Integer superSampling) {
        this.superSampling = superSampling;
    }

    public VariableConfiguration[] getVariableConfigurations() {
        return variableConfigurations;
    }

    public void setVariableConfigurations(VariableConfiguration... variableConfigurations) {
        this.variableConfigurations = variableConfigurations;
    }

    public AggregatorConfiguration[] getAggregatorConfigurations() {
        return aggregatorConfigurations;
    }

    public void setAggregatorConfigurations(AggregatorConfiguration... aggregatorConfigurations) {
        this.aggregatorConfigurations = aggregatorConfigurations;
    }

    public BinnerContext createBinningContext() {
        VariableContext varCtx = createVariableContext();
        return new BinnerContextImpl(createBinningGrid(),
                                      varCtx,
                                      createBinManager(varCtx));
    }

    public BinningGrid createBinningGrid() {
        if (numRows == 0) {
            numRows = IsinBinningGrid.DEFAULT_NUM_ROWS;
        }
        return new IsinBinningGrid(numRows);
    }

    private BinManager createBinManager(VariableContext varCtx) {
        Aggregator[] aggregators = createAggregators(varCtx);
        return createBinManager(aggregators);
    }

    public Aggregator[] createAggregators(VariableContext varCtx) {
        Aggregator[] aggregators = new Aggregator[aggregatorConfigurations.length];
        for (int i = 0; i < aggregators.length; i++) {
            AggregatorConfiguration aggregatorConfiguration = aggregatorConfigurations[i];
            AggregatorDescriptor descriptor = AggregatorDescriptorRegistry.getInstance().getAggregatorDescriptor(aggregatorConfiguration.type);
            if (descriptor != null) {
                aggregators[i] = descriptor.createAggregator(varCtx, PropertyContainer.createObjectBacked(aggregatorConfiguration));
            } else {
                throw new IllegalArgumentException("Unknown aggregator type: " + aggregatorConfiguration.type);
            }
        }
        return aggregators;
    }

    protected BinManager createBinManager(Aggregator[] aggregators) {
        return new BinManagerImpl(aggregators);
    }

    public VariableContext createVariableContext() {
        VariableContextImpl variableContext = new VariableContextImpl();
        if (maskExpr == null) {
            maskExpr = "";
        }
        variableContext.setMaskExpr(maskExpr);

        // define declared variables
        //
        if (variableConfigurations != null) {
            for (VariableConfiguration variable : variableConfigurations) {
                variableContext.defineVariable(variable.name, variable.expr);
            }
        }

        // define variables of all aggregators
        //
        if (aggregatorConfigurations != null) {
            for (AggregatorConfiguration aggregatorConfiguration : aggregatorConfigurations) {
                String varName = aggregatorConfiguration.varName;
                if (varName != null) {
                    variableContext.defineVariable(varName);
                } else {
                    String[] varNames = aggregatorConfiguration.varNames;
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

    public static BinnerConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new BinnerConfig());
    }

    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (BindingException e) {
            throw new RuntimeException(e);
        }
    }

    public static class VariableConfiguration {
        private String name;

        private String expr;

        public VariableConfiguration() {
        }

        public VariableConfiguration(String name, String expr) {
            this.name = name;
            this.expr = expr;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getExpr() {
            return expr;
        }

        public void setExpr(String expr) {
            this.expr = expr;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VariableConfiguration that = (VariableConfiguration) o;

            if (!expr.equals(that.expr)) return false;
            if (!name.equals(that.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + expr.hashCode();
            return result;
        }
    }

    public static class AggregatorConfiguration {
        private String type;

        private String varName;

        private String[] varNames;

        private Integer percentage;

        private Double weightCoeff;

        private Float fillValue;

        public AggregatorConfiguration() {
        }

        public AggregatorConfiguration(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getVarName() {
            return varName;
        }

        public void setVarName(String varName) {
            this.varName = varName;
        }

        public String[] getVarNames() {
            return varNames;
        }

        public void setVarNames(String[] varNames) {
            this.varNames = varNames;
        }

        public Integer getPercentage() {
            return percentage;
        }

        public void setPercentage(Integer percentage) {
            this.percentage = percentage;
        }

        public Double getWeightCoeff() {
            return weightCoeff;
        }

        public void setWeightCoeff(Double weightCoeff) {
            this.weightCoeff = weightCoeff;
        }

        public Float getFillValue() {
            return fillValue;
        }

        public void setFillValue(Float fillValue) {
            this.fillValue = fillValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AggregatorConfiguration that = (AggregatorConfiguration) o;

            if (fillValue != null ? !fillValue.equals(that.fillValue) : that.fillValue != null) return false;
            if (percentage != null ? !percentage.equals(that.percentage) : that.percentage != null) return false;
            if (!type.equals(that.type)) return false;
            if (varName != null ? !varName.equals(that.varName) : that.varName != null) return false;
            if (!Arrays.equals(varNames, that.varNames)) return false;
            if (weightCoeff != null ? !weightCoeff.equals(that.weightCoeff) : that.weightCoeff != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + (varName != null ? varName.hashCode() : 0);
            result = 31 * result + (varNames != null ? Arrays.hashCode(varNames) : 0);
            result = 31 * result + (percentage != null ? percentage.hashCode() : 0);
            result = 31 * result + (weightCoeff != null ? weightCoeff.hashCode() : 0);
            result = 31 * result + (fillValue != null ? fillValue.hashCode() : 0);
            return result;
        }
    }

}
