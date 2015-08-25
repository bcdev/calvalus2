package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import com.bc.calvalus.wpsrest.CalvalusParameter;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.Processor;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class transform the input parameters map into a format recognized by Calvalus Production Request.
 * <p/>
 * Created by hans on 23.07.2015.
 */
public class CalvalusDataInputs {

    private final Map<String, String> inputMapRaw;
    private final Map<String, String> inputMapFormatted;

    public CalvalusDataInputs(ExecuteRequestExtractor executeRequestExtractor, Processor processor) {
        this.inputMapFormatted = new HashMap<>();
        this.inputMapRaw = executeRequestExtractor.getInputParametersMapRaw();
        extractProductionParameters();
        extractProcessorInfoParameters();
        extractProductsetParameters();
        transformProcessorParameters(processor);
        transformL3Parameters();
        this.inputMapFormatted.put("autoStaging", "true");
    }

    private void transformL3Parameters() {
        inputMapFormatted.put("calvalus.l3.parameters", inputMapRaw.get("calvalus.l3.parameters"));
    }

    private void extractProductionParameters() {
        List<String> productionParameterNames = CalvalusParameter.getProductionParameters();
        for (String parameterName : productionParameterNames) {
            if (StringUtils.isNotBlank(inputMapRaw.get(parameterName))) {
                inputMapFormatted.put(parameterName, inputMapRaw.get(parameterName));
            }
        }
    }

    private void extractProcessorInfoParameters() {
        List<String> processorInfoParameterNames = CalvalusParameter.getProcessorInfoParameters();
        for (String parameterName : processorInfoParameterNames) {
            if (StringUtils.isNotBlank(inputMapRaw.get(parameterName))) {
                inputMapFormatted.put(parameterName, inputMapRaw.get(parameterName));
            }
        }
    }

    private void extractProductsetParameters() {
        List<String> productsetParameterNames = CalvalusParameter.getProductsetParameters();
        for (String parameterName : productsetParameterNames) {
            if (StringUtils.isNotBlank(inputMapRaw.get(parameterName))) {
                inputMapFormatted.put(parameterName, inputMapRaw.get(parameterName));
            }
        }
    }

    private void transformProcessorParameters(Processor processor) {
        if (processor != null) {
            ParameterDescriptor[] processorParameters = processor.getParameterDescriptors();
            StringBuilder sb = new StringBuilder();
            sb.append("<parameters>\n");
            if (processorParameters != null) {
                for (ParameterDescriptor parameterDescriptor : processorParameters) {
                    sb.append("<");
                    sb.append(parameterDescriptor.getName());
                    sb.append(">");

                    sb.append(inputMapRaw.get(parameterDescriptor.getName()));

                    sb.append("</");
                    sb.append(parameterDescriptor.getName());
                    sb.append(">\n");
                }
                sb.append("</parameters>");
                this.inputMapFormatted.put("processorParameters", sb.toString());
            } else {
                this.inputMapFormatted.put("processorParameters", processor.getDefaultParameters());
            }
        }
    }

    /**
     * Returns a value for the given parameter name.
     *
     * @param parameterName Parameter name.
     *
     * @return The corresponding value.
     */
    public String getValue(String parameterName) {
        return inputMapFormatted.get(parameterName);
    }

    /**
     * Returns a key value pair of the input data.
     *
     * @return A Map object that consists of key value pair of the input data.
     */
    public Map<String, String> getInputMapFormatted() {
        return inputMapFormatted;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (String key : inputMapFormatted.keySet()) {
            stringBuilder.append(key);
            stringBuilder.append(" : ");
            stringBuilder.append(inputMapFormatted.get(key));
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}
