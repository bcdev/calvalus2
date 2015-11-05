/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.support.VariableContextImpl;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

/**
 * A configuration for Mosaic processing
 *
 * @author MarcoZ
 */
public class MosaicConfig implements XmlConvertible {

    @Parameter
    private String algorithmName;

    @Parameter
    private String validMaskExpression;

    @Parameter
    private String[] variableNames;

    @Parameter
    private String[] virtualVariableNames = null;

    @Parameter
    private String[] virtualVariableExpr = null;

    public MosaicConfig() {
    }

    public MosaicConfig(String algorithmName, String validMaskExpression, String[] variableNames, String[] virtualVariableNames, String[] virtualVariableExpr) {
        this(algorithmName, validMaskExpression, variableNames);
        this.virtualVariableNames = virtualVariableNames;
        this.virtualVariableExpr = virtualVariableExpr;
    }

    public MosaicConfig(String algorithmName, String validMaskExpression, String[] variableNames) {
        this.algorithmName = algorithmName;
        this.validMaskExpression = validMaskExpression;
        this.variableNames = variableNames;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public String getValidMaskExpression() {
        return validMaskExpression;
    }

    public String[] getVariableNames() {
        return variableNames;
    }

    public static MosaicConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new MosaicConfig());
    }

    @Override
    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (ConversionException e) {
            throw new RuntimeException(e);
        }
    }

    public static MosaicConfig get(Configuration jobConfig) {
        String xml = jobConfig.get(JobConfigNames.CALVALUS_MOSAIC_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException("Missing Mosaic configuration '" + JobConfigNames.CALVALUS_MOSAIC_PARAMETERS + "'");
        }
        try {
            return fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid Mosaic configuration: " + e.getMessage(), e);
        }
    }

    public static MosaicAlgorithm createAlgorithm(Configuration jobConf) {
        MosaicConfig mosaicConfig = MosaicConfig.get(jobConf);
        Class<?> algorithmClass = null;
        try {
            algorithmClass = Class.forName(mosaicConfig.getAlgorithmName());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("mosaic algorithm class " + algorithmClass + " not found.", e);
        }
        MosaicAlgorithm mosaicAlgorithm = (MosaicAlgorithm) ReflectionUtils.newInstance(algorithmClass, jobConf);
        mosaicAlgorithm.setVariableContext(mosaicConfig.createVariableContext());
        return mosaicAlgorithm;
    }

    public VariableContext createVariableContext() {
        VariableContextImpl variableContext = new VariableContextImpl();
        if (validMaskExpression == null) {
            variableContext.setMaskExpr("");
        } else {
            variableContext.setMaskExpr(validMaskExpression);
        }

        if (variableNames != null) {
            for (String variableName : variableNames) {
                variableContext.defineVariable(variableName);
            }
        }
        if (virtualVariableNames != null && virtualVariableExpr != null) {
            for (int i = 0; i < virtualVariableNames.length; i++) {
                String name = virtualVariableNames[i];
                String expr = virtualVariableExpr[i];
                variableContext.defineVariable(name, expr);
            }
        }
        return variableContext;
    }
}
