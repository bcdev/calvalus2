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


import com.bc.calvalus.binning.Aggregator;
import com.bc.calvalus.binning.AggregatorAverage;
import com.bc.calvalus.binning.AggregatorAverageML;
import com.bc.calvalus.binning.AggregatorMinMax;
import com.bc.calvalus.binning.AggregatorOnMaxSet;
import com.bc.calvalus.binning.AggregatorPercentile;
import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.BinManagerImpl;
import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.binning.BinningContextImpl;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.IsinBinningGrid;
import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.binning.VariableContextImpl;
import com.bc.calvalus.processing.beam.BeamUtils;
import org.esa.beam.framework.gpf.annotations.Parameter;

public class L3Config {
    public static final String L3_REQUEST_FILENAME = "wps-request.xml";

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

    @Parameter
    int numRows;
    @Parameter
    Integer superSampling;
    @Parameter
    String maskExpr;
    @Parameter(itemAlias = "variable")
    VariableConfiguration[] variables;
    @Parameter(itemAlias = "aggregator")
    AggregatorConfiguration[] aggregators;

    public static L3Config fromXml(String level3ParametersXml) {
        L3Config l3Config = new L3Config();
        BeamUtils.convertXmlToObject(level3ParametersXml, l3Config);
        return l3Config;
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

    public void setVariables(VariableConfiguration[] variables) {
        this.variables = variables;
    }

    public void setAggregators(AggregatorConfiguration[] aggregators) {
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
            String type = aggregators[i].type;
            Aggregator aggregator;
            if (type.equals("AVG")) {
                aggregator = getAggregatorAverage(varCtx, aggregators[i]);
            } else if (type.equals("AVG_ML")) {
                aggregator = getAggregatorAverageML(varCtx, aggregators[i]);
            } else if (type.equals("MIN_MAX")) {
                aggregator = getAggregatorMinMax(varCtx, aggregators[i]);
            } else if (type.equals("ON_MAX_SET")) {
                aggregator = getAggregatorOnMaxSet(varCtx, aggregators[i]);
            } else if (type.equals("PERCENTILE")) {
                aggregator = getAggregatorPercentile(varCtx, aggregators[i]);
            } else {
                throw new IllegalArgumentException("Unknown aggregator type: " + type);
            }
            aggs[i] = aggregator;
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

    private Aggregator getAggregatorAverage(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorAverage(varCtx, aggregatorConf.varName, aggregatorConf.weightCoeff, aggregatorConf.fillValue);
    }

    private Aggregator getAggregatorAverageML(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorAverageML(varCtx, aggregatorConf.varName, aggregatorConf.weightCoeff, aggregatorConf.fillValue);
    }

    private Aggregator getAggregatorMinMax(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorMinMax(varCtx, aggregatorConf.varName, aggregatorConf.fillValue);
    }

    private Aggregator getAggregatorOnMaxSet(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorOnMaxSet(varCtx, aggregatorConf.varNames);
    }

    private Aggregator getAggregatorPercentile(VariableContext varCtx, AggregatorConfiguration aggregatorConf) {
        return new AggregatorPercentile(varCtx, aggregatorConf.varName, aggregatorConf.percentage, aggregatorConf.fillValue);
    }
}
