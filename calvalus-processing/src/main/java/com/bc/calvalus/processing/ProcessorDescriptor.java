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

package com.bc.calvalus.processing;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.gpf.annotations.Parameter;

import java.util.HashMap;
import java.util.Map;

public class ProcessorDescriptor {

    public static class Variable {

        private String name;

        private String defaultAggregator;
        private String defaultWeightCoeff;

        // empty constructor for XML serialization
        public Variable() {
        }

        public Variable(String name, String defaultAggregator, String defaultWeightCoeff) {
            this.name = name;
            this.defaultAggregator = defaultAggregator;
            this.defaultWeightCoeff = defaultWeightCoeff;
        }

        public String getName() {
            return name;
        }

        public String getDefaultAggregator() {
            return defaultAggregator;
        }

        public String getDefaultWeightCoeff() {
            return defaultWeightCoeff;
        }

    }

    public static class JobParameter {

        private String name;
        private String value;
    }

    public static class ParameterDescriptor {
        private String name;
        private String type;
        private String description;
        private String defaultValue;
        private String[] valueSet;

        // empty constructor for XML serialization
        public ParameterDescriptor() {
        }

        public ParameterDescriptor(String name, String type, String description, String defaultValue, String[] valueSet) {
            this.name = name;
            this.type = type;
            this.description = description;
            this.defaultValue = defaultValue;
            this.valueSet = valueSet;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public String getType() {
            return type;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public String[] getValueSet() {
            return valueSet;
        }
    }

    @Parameter
    private String executableName;

    @Parameter
    private String processorName;
    @Parameter
    private String processorVersion;
    @Parameter
    private String defaultParameters;
    @Parameter
    private String[] outputFormats;
    // Short description in XHTML
    @Parameter
    private String descriptionHtml;

    @Parameter
    private String outputVariableDefaultMaskExpr;

    @Deprecated
    @Parameter
    private String validMaskExpression;

    @Parameter(itemAlias = "outputVariable")
    private Variable[] outputVariables;

    @Parameter
    private String[] inputProductTypes;

    @Parameter
    private String outputProductType;

    @Parameter(defaultValue = "false")
    private boolean formattingMandatory;


    @Parameter(itemAlias = "jobParameter")
    private JobParameter[] jobConfig;

    @Parameter(itemAlias = "parameterDescriptor")
    private ParameterDescriptor[] parameterDescriptors;

    // empty constructor for XML serialization
    public ProcessorDescriptor() {
    }

    public ProcessorDescriptor(String executableName,
                               String processorName,
                               String processorVersion,
                               String defaultParameters,
                               Variable... outputVariables) {

        Assert.notNull(executableName, "executableName");
        Assert.notNull(processorName, "processorName");
        Assert.notNull(processorVersion, "processorVersion");
        Assert.notNull(defaultParameters, "defaultParameters");
        this.executableName = executableName;
        this.processorName = processorName;
        this.processorVersion = processorVersion;
        this.defaultParameters = defaultParameters;
        this.outputVariables = outputVariables;
    }

    public String getExecutableName() {
        return executableName;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getProcessorVersion() {
        return processorVersion;
    }

    public String getDefaultParameters() {
        return defaultParameters;
    }

    public String[] getOutputFormats() {
        return outputFormats;
    }

    public boolean isFormattingMandatory() {
        return formattingMandatory;
    }

    public String getDescriptionHtml() {
        return descriptionHtml;
    }

    public String getMaskExpression() {
        return outputVariableDefaultMaskExpr != null ? outputVariableDefaultMaskExpr : validMaskExpression;
    }

    public Variable[] getOutputVariables() {
        return outputVariables;
    }

    public String[] getInputProductTypes() {
        return inputProductTypes;
    }

    public String getOutputProductType() {
        return outputProductType;
    }

    public Map<String, String> getJobConfiguration() {
        HashMap<String, String> map = new HashMap<String, String>();
        if (jobConfig != null) {
            for (JobParameter jobParameter : jobConfig) {
                map.put(jobParameter.name, jobParameter.value);
            }
        }
        return map;
    }

    public ParameterDescriptor[] getParameterDescriptors() {
        return parameterDescriptors;
    }

    // only used in tests

    public void setOutputProductType(String outputProductType) {
        this.outputProductType = outputProductType;
    }

    public void setParameterDescriptors(ParameterDescriptor...parameterDescriptors) {
        this.parameterDescriptors = parameterDescriptors;
    }
}
