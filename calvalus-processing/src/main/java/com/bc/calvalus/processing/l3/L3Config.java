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


import com.bc.calvalus.binning.BinnerContext;
import com.bc.calvalus.binning.BinnerContextImpl;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.BinnerConfig;

@SuppressWarnings({"UnusedDeclaration"})
public class L3Config implements XmlConvertible {

    private BinnerConfig binnerConfig;

    public static L3Config get(Configuration jobConfig) {
        String xml = jobConfig.get(JobConfigNames.CALVALUS_L3_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException("Missing L3 configuration '" + JobConfigNames.CALVALUS_L3_PARAMETERS + "'");
        }
        try {
            return fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid L3 configuration: " + e.getMessage(), e);
        }
    }

    public L3Config() {
        this(new BinnerConfig());
    }

    private L3Config(BinnerConfig binnerConfig) {
        this.binnerConfig = binnerConfig;
    }

    public BinnerConfig getBinnerConfig() {
        return binnerConfig;
    }

    public static L3Config fromXml(String xml) throws BindingException {
        return new L3Config(BinnerConfig.fromXml(xml));
    }

    @Override
    public String toXml() {
        return binnerConfig.toXml();
    }

    public int getNumRows() {
        return binnerConfig.getNumRows();
    }

    public void setNumRows(int numRows) {
        binnerConfig.setNumRows(numRows);
    }

    public Integer getSuperSampling() {
        return binnerConfig.getSuperSampling();
    }

    public void setSuperSampling(Integer superSampling) {
        binnerConfig.setSuperSampling(superSampling);
    }

    public String getMaskExpr() {
        return binnerConfig.getMaskExpr();
    }

    public void setMaskExpr(String maskExpr) {
        binnerConfig.setMaskExpr(maskExpr);
    }

    public BinnerConfig.VariableConfiguration[] getVariableConfigurations() {
        return binnerConfig.getVariableConfigurations();
    }

    public void setVariableConfigurations(BinnerConfig.VariableConfiguration... variables) {
        binnerConfig.setVariableConfigurations(variables);
    }

    public BinnerConfig.AggregatorConfiguration[] getAggregatorConfigurations() {
        return binnerConfig.getAggregatorConfigurations();
    }

    public void setAggregatorConfigurations(BinnerConfig.AggregatorConfiguration... aggregators) {
        binnerConfig.setAggregatorConfigurations(aggregators);
    }

    public BinnerContext getBinningContext() {
        VariableContext varCtx = getVariableContext();
        return new BinnerContextImpl(getBinningGrid(),
                                      varCtx,
                                      new L3BinManagerImpl(binnerConfig.createAggregators(varCtx)));
    }

    public BinningGrid getBinningGrid() {
        return binnerConfig.createBinningGrid();
    }

    public VariableContext getVariableContext() {
        return binnerConfig.createVariableContext();
    }

}
