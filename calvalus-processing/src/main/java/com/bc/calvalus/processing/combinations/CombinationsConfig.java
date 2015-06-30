package com.bc.calvalus.processing.combinations;

import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.framework.gpf.annotations.Parameter;
import org.esa.snap.framework.gpf.annotations.ParameterBlockConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * The configuration for iterating over all possible combinations of the variables.
 */
public class CombinationsConfig implements XmlConvertible {

    public static class Variable {
        @Parameter
        private String name;
        @Parameter
        private String[] values;
        @Parameter
        private String loopLocation;

        // for deserialization
        public Variable() {
        }

        public Variable(String name, String loopLocation, String...values) {
            this.name = name;
            this.values = values;
            this.loopLocation = loopLocation;
        }

        public String getName() {
            return name;
        }

        public String[] getValues() {
            return values;
        }

        public String getLoopLocation() {
            return loopLocation;
        }
    }

    @Parameter(itemAlias = "variable")
    private Variable[] variables;

    // for deserialization
    public CombinationsConfig() {
        this(new Variable[0]);
    }

    public CombinationsConfig(Variable... variables) {
        this.variables = variables;
    }

    public Variable[] getVariables() {
        return variables;
    }

    public List<String> getVariableNames() {
        List<String> variableNames = new ArrayList<>();
        for (Variable variable : variables) {
            variableNames.add(variable.name);
        }
        return variableNames;
    }

    public static CombinationsConfig get(Configuration jobConfig) {
        String xml = jobConfig.get(CombinationsWorkflowItem.COMBINATION_CONFIG);
        if (xml == null) {
            throw new IllegalArgumentException(
                    "Missing combinations configuration '" + CombinationsWorkflowItem.COMBINATION_CONFIG + "'");
        }
        try {
            return fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid multiformat configuration: " + e.getMessage(), e);
        }
    }

    public static CombinationsConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new CombinationsConfig());
    }

    @Override
    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (ConversionException e) {
            throw new RuntimeException(e);
        }
    }
}
