package com.bc.calvalus.processing;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.gpf.annotations.Parameter;

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

    // empty constructor for XML serialization
    public ProcessorDescriptor() {
    }

    public ProcessorDescriptor(String executableName,
                               String processorName,
                               String processorVersion,
                               String defaultParameters,
                               Variable ... outputVariables) {

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

}
