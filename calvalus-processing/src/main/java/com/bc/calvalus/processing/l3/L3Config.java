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


import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.Aggregator;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.PlanetaryGrid;
import org.esa.beam.binning.VariableContext;
import org.esa.beam.binning.AggregatorConfig;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.binning.operator.VariableConfig;
import org.esa.beam.binning.support.BinningContextImpl;

// todo - remove class (BinningConfig should be a sufficient replacement) (nf, 2012-02-14)

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
        return getBinningConfig().toXml();
    }

    public int getNumRows() {
        return getBinningConfig().getNumRows();
    }

    public void setNumRows(int numRows) {
        getBinningConfig().setNumRows(numRows);
    }

    public Integer getSuperSampling() {
        return getBinningConfig().getSuperSampling();
    }

    public void setSuperSampling(Integer superSampling) {
        getBinningConfig().setSuperSampling(superSampling);
    }

    public String getMaskExpr() {
        return getBinningConfig().getMaskExpr();
    }

    public void setMaskExpr(String maskExpr) {
        getBinningConfig().setMaskExpr(maskExpr);
    }

    public VariableConfig[] getVariableConfigs() {
        return getBinningConfig().getVariableConfigs();
    }

    public void setVariableConfigs(VariableConfig... variables) {
        getBinningConfig().setVariableConfigs(variables);
    }

    public AggregatorConfig[] getAggregatorConfigs() {
        return getBinningConfig().getAggregatorConfigs();
    }

    public void setAggregatorConfigs(AggregatorConfig... aggregators) {
        getBinningConfig().setAggregatorConfigs(aggregators);
    }

    public BinningContext createBinningContext() {
        VariableContext variableContext = createVariableContext();
        Aggregator[] aggregators = binningConfig.createAggregators(variableContext);
        L3BinManagerImpl binManager = new L3BinManagerImpl(variableContext, binningConfig.getPostProcessorConfig(), aggregators);
        return new BinningContextImpl(createPlanetaryGrid(),
                                      binManager,
                                      binningConfig.getCompositingType(),
                                      getSuperSampling() != null ? getSuperSampling() : 1);
    }

    public PlanetaryGrid createPlanetaryGrid() {
        return getBinningConfig().createPlanetaryGrid();
    }

    public VariableContext createVariableContext() {
        return getBinningConfig().createVariableContext();
    }

}
