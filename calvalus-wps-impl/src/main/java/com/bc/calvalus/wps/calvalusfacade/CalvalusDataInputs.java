package com.bc.calvalus.wps.calvalusfacade;


import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.BEAM_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.CALVALUS_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.INPUT_DATASET;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.getProductionInfoParameters;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.getProductsetParameters;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import com.bc.calvalus.wps.exceptions.WpsInvalidParameterValueException;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class transform the input parameters map into a format recognized by Calvalus Production Request.
 *
 * @author hans
 */
public class CalvalusDataInputs {

    private final Map<String, String> inputMapRaw;
    private final Map<String, String> inputMapFormatted;

    public CalvalusDataInputs(ExecuteRequestExtractor executeRequestExtractor, CalvalusProcessor calvalusProcessor, ProductSet[] productSets) {
        this.inputMapFormatted = new HashMap<>();
        this.inputMapRaw = executeRequestExtractor.getInputParametersMapRaw();
        extractProductionParameters();
        if (calvalusProcessor != null) {
            extractProductionInfoParameters(calvalusProcessor);
            extractProcessorInfoParameters(calvalusProcessor);
            transformProcessorParameters(calvalusProcessor);
        }
        extractProductsetParameters(productSets, calvalusProcessor);
        extractL3Parameters();
        this.inputMapFormatted.put("autoStaging", "true");
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

    private void extractL3Parameters() {
        if (StringUtils.isNotBlank(inputMapRaw.get("calvalus.l3.parameters"))) {
            inputMapFormatted.put("calvalus.l3.parameters", inputMapRaw.get("calvalus.l3.parameters"));
        }
    }

    private void extractProductionParameters() {
        List<String> productionParameterNames = getProductionInfoParameters();
        for (String parameterName : productionParameterNames) {
            if (StringUtils.isNotBlank(inputMapRaw.get(parameterName))) {
                inputMapFormatted.put(parameterName, inputMapRaw.get(parameterName));
            }
        }
    }

    private void extractProductionInfoParameters(CalvalusProcessor calvalusProcessor) {
        if (calvalusProcessor.getDefaultCalvalusBundle() != null) {
            inputMapFormatted.put(CALVALUS_BUNDLE_VERSION.getIdentifier(),
                                  calvalusProcessor.getDefaultCalvalusBundle());
        }
        if (calvalusProcessor.getDefaultBeamBundle() != null) {
            inputMapFormatted.put(BEAM_BUNDLE_VERSION.getIdentifier(), calvalusProcessor.getDefaultBeamBundle());
        }
    }

    private void extractProcessorInfoParameters(CalvalusProcessor calvalusProcessor) {
        inputMapFormatted.put(PROCESSOR_BUNDLE_NAME.getIdentifier(), calvalusProcessor.getBundleName());
        inputMapFormatted.put(PROCESSOR_BUNDLE_VERSION.getIdentifier(), calvalusProcessor.getBundleVersion());
        inputMapFormatted.put(PROCESSOR_NAME.getIdentifier(), calvalusProcessor.getName());
    }

    private void extractProductsetParameters(ProductSet[] productSets, CalvalusProcessor calvalusProcessor) {
        String dataSetName = inputMapRaw.get(INPUT_DATASET.getIdentifier());
        for (ProductSet productSet : productSets) {
            if (productSet.getName().equals(dataSetName)
                && ArrayUtils.contains(calvalusProcessor.getInputProductTypes(), productSet.getProductType())) {
                inputMapFormatted.put("inputPath", "/calvalus/" + productSet.getPath());
            }
        }

        if (calvalusProcessor != null && StringUtils.isBlank(inputMapFormatted.get("inputPath"))) {
            throw new WpsInvalidParameterValueException(INPUT_DATASET.getIdentifier());
        }

        List<String> productsetParameterNames = getProductsetParameters();
        for (String parameterName : productsetParameterNames) {
            if (StringUtils.isNotBlank(inputMapRaw.get(parameterName))) {
                inputMapFormatted.put(parameterName, inputMapRaw.get(parameterName));
            }
        }
    }

    private void transformProcessorParameters(CalvalusProcessor calvalusProcessor) {
        ParameterDescriptor[] processorParameters = calvalusProcessor.getParameterDescriptors();
        StringBuilder sb = new StringBuilder();
        sb.append("<parameters>\n");
        if (processorParameters != null) {
            for (ParameterDescriptor parameterDescriptor : processorParameters) {
                String processorParameterValue = inputMapRaw.get(parameterDescriptor.getName());
                if (StringUtils.isNotBlank(processorParameterValue)) {
                    sb.append("<");
                    sb.append(parameterDescriptor.getName());
                    sb.append(">");

                    sb.append(processorParameterValue);

                    sb.append("</");
                    sb.append(parameterDescriptor.getName());
                    sb.append(">\n");
                }
            }
            sb.append("</parameters>");
            this.inputMapFormatted.put("processorParameters", sb.toString());
        } else {
            this.inputMapFormatted.put("processorParameters", calvalusProcessor.getDefaultParameters());
        }
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
