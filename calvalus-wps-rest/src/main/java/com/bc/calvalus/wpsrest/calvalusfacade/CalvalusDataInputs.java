package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.exception.WpsInvalidParameterValueException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bc.calvalus.wpsrest.CalvalusParameter.*;

/**
 * This class transform the input parameters map into a format recognized by Calvalus Production Request.
 * <p/>
 * Created by hans on 23.07.2015.
 */
public class CalvalusDataInputs {

    private final Map<String, String> inputMapRaw;
    private final Map<String, String> inputMapFormatted;

    public CalvalusDataInputs(ExecuteRequestExtractor executeRequestExtractor, Processor processor, ProductSet[] productSets) {
        this.inputMapFormatted = new HashMap<>();
        this.inputMapRaw = executeRequestExtractor.getInputParametersMapRaw();
        extractProductionParameters();
        extractProductionInfoParameters(processor);
        extractProcessorInfoParameters(processor);
        extractProductsetParameters(productSets, processor);
        transformProcessorParameters(processor);
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

    private void extractProductionInfoParameters(Processor processor) {
        if (processor != null) {
            inputMapFormatted.put(CALVALUS_BUNDLE_VERSION.getIdentifier(), processor.getDefaultCalvalusBundle());
            inputMapFormatted.put(SNAP_BUNDLE_VERSION.getIdentifier(), processor.getDefaultSnapBundle());
        }
    }

    private void extractProcessorInfoParameters(Processor processor) {
        if (processor != null) {
            inputMapFormatted.put(PROCESSOR_BUNDLE_NAME.getIdentifier(), processor.getBundleName());
            inputMapFormatted.put(PROCESSOR_BUNDLE_VERSION.getIdentifier(), processor.getBundleVersion());
            inputMapFormatted.put(PROCESSOR_NAME.getIdentifier(), processor.getName());
        }
    }

    private void extractProductsetParameters(ProductSet[] productSets, Processor processor) {
        String dataSetName = inputMapRaw.get(INPUT_DATASET.getIdentifier());
        for (ProductSet productSet : productSets) {
            if (productSet.getName().equals(dataSetName)
                && ArrayUtils.contains(processor.getInputProductTypes(), productSet.getProductType())) {
                inputMapFormatted.put("inputPath", "/calvalus/" + productSet.getPath());
            }
        }

        if (processor != null && StringUtils.isBlank(inputMapFormatted.get("inputPath"))) {
            throw new WpsInvalidParameterValueException(INPUT_DATASET.getIdentifier());
        }

        List<String> productsetParameterNames = getProductsetParameters();
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
                this.inputMapFormatted.put("processorParameters", processor.getDefaultParameters());
            }
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
