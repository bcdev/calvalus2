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


import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.binning.BinningContextImpl;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.BinningConfig;

@SuppressWarnings({"UnusedDeclaration"})
public class L3Config implements XmlConvertible {

    private BinningConfig binningConfig;

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
        this(new BinningConfig());
    }

    private L3Config(BinningConfig binningConfig) {
        this.binningConfig = binningConfig;
    }

    public BinningConfig getBinningConfig() {
        return binningConfig;
    }

    public static L3Config fromXml(String xml) throws BindingException {
        return new L3Config(BinningConfig.fromXml(xml));
    }

    @Override
    public String toXml() {
        return binningConfig.toXml();
    }

    public int getNumRows() {
        return binningConfig.getNumRows();
    }

    public void setNumRows(int numRows) {
        binningConfig.setNumRows(numRows);
    }

    public Integer getSuperSampling() {
        return binningConfig.getSuperSampling();
    }

    public void setSuperSampling(Integer superSampling) {
        binningConfig.setSuperSampling(superSampling);
    }

    public String getMaskExpr() {
        return binningConfig.getMaskExpr();
    }

    public void setMaskExpr(String maskExpr) {
        binningConfig.setMaskExpr(maskExpr);
    }

    public BinningConfig.VariableConfiguration[] getVariableConfigurations() {
        return binningConfig.getVariableConfigurations();
    }

    public void setVariableConfigurations(BinningConfig.VariableConfiguration... variables) {
        binningConfig.setVariableConfigurations(variables);
    }

    public BinningConfig.AggregatorConfiguration[] getAggregatorConfigurations() {
        return binningConfig.getAggregatorConfigurations();
    }

    public void setAggregatorConfigurations(BinningConfig.AggregatorConfiguration... aggregators) {
        binningConfig.setAggregatorConfigurations(aggregators);
    }

    public BinningContext getBinningContext() {
        VariableContext varCtx = getVariableContext();
        return new BinningContextImpl(getBinningGrid(),
                                      varCtx,
                                      new L3BinManagerImpl(binningConfig.getAggregators(varCtx)));
    }

    public BinningGrid getBinningGrid() {
        return binningConfig.getBinningGrid();
    }

    public VariableContext getVariableContext() {
        return binningConfig.getVariableContext();
    }

}
