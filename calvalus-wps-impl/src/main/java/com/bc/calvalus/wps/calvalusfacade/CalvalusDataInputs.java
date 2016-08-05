package com.bc.calvalus.wps.calvalusfacade;


import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.CALVALUS_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.INPUT_DATASET;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.SNAP_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.getProductionInfoParameters;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.getProductsetParameters;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.esa.snap.core.datamodel.ProductData;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class transform the input parameters map into a format recognized by Calvalus Production Request.
 *
 * @author hans
 */
public class CalvalusDataInputs {

    public static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat("yyyy-MM-dd");
    public static final int MIN_DATE = 0;
    public static final long MAX_DATE = 4133894400000L;

    private final Map<String, String> inputMapRaw;
    private final Map<String, String> inputMapFormatted;

    public CalvalusDataInputs(ExecuteRequestExtractor executeRequestExtractor, CalvalusProcessor calvalusProcessor, ProductSet[] productSets) throws InvalidParameterValueException {
        this.inputMapFormatted = new HashMap<>();
        this.inputMapRaw = executeRequestExtractor.getInputParametersMapRaw();
        extractProductionParameters();
        if (calvalusProcessor != null) {
            extractProductionInfoParameters(calvalusProcessor);
            extractProcessorInfoParameters(calvalusProcessor);
            transformProcessorParameters(calvalusProcessor);
        }
        extractProductSetParameters(productSets, calvalusProcessor);
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
        productionParameterNames.stream()
                    .filter(parameterName -> StringUtils.isNotBlank(inputMapRaw.get(parameterName)))
                    .forEach(parameterName -> inputMapFormatted.put(parameterName, inputMapRaw.get(parameterName)));
    }

    private void extractProductionInfoParameters(CalvalusProcessor calvalusProcessor) {
        if (calvalusProcessor.getDefaultCalvalusBundle() != null) {
            inputMapFormatted.put(CALVALUS_BUNDLE_VERSION.getIdentifier(),
                                  calvalusProcessor.getDefaultCalvalusBundle());
        }
        if (calvalusProcessor.getDefaultSnapBundle() != null) {
            inputMapFormatted.put(SNAP_BUNDLE_VERSION.getIdentifier(), calvalusProcessor.getDefaultSnapBundle());
        }
    }

    private void extractProcessorInfoParameters(CalvalusProcessor calvalusProcessor) {
        inputMapFormatted.put(PROCESSOR_BUNDLE_NAME.getIdentifier(), calvalusProcessor.getBundleName());
        inputMapFormatted.put(PROCESSOR_BUNDLE_VERSION.getIdentifier(), calvalusProcessor.getBundleVersion());
        inputMapFormatted.put(PROCESSOR_NAME.getIdentifier(), calvalusProcessor.getName());
        inputMapFormatted.put("processorBundleLocation", calvalusProcessor.getBundleLocation());
    }

    private void extractProductSetParameters(ProductSet[] productSets, CalvalusProcessor calvalusProcessor) throws InvalidParameterValueException {
        String dataSetName = inputMapRaw.get(INPUT_DATASET.getIdentifier());
        List<String> productSetParameterNames = getProductsetParameters();
        productSetParameterNames.stream()
                    .filter(parameterName -> StringUtils.isNotBlank(inputMapRaw.get(parameterName)))
                    .forEach(parameterName -> inputMapFormatted.put(parameterName, inputMapRaw.get(parameterName)));
        for (ProductSet productSet : productSets) {
            if (productSet.getName().equals(dataSetName)
                && ArrayUtils.contains(calvalusProcessor.getInputProductTypes(), productSet.getProductType())) {
                inputMapFormatted.put("inputPath", "/calvalus/" + productSet.getPath());
                Date minDate = productSet.getMinDate() == null ? new Date(MIN_DATE) : productSet.getMinDate();
                Date maxDate = productSet.getMaxDate() == null ? new Date(MAX_DATE) : productSet.getMaxDate();
                inputMapFormatted.putIfAbsent("minDateSource", DATE_FORMAT.format(minDate));
                inputMapFormatted.putIfAbsent("maxDateSource", DATE_FORMAT.format(maxDate));
                break;
            }
        }

        if (calvalusProcessor != null && StringUtils.isBlank(inputMapFormatted.get("inputPath"))) {
            throw new InvalidParameterValueException(INPUT_DATASET.getIdentifier());
        }
    }

    private void transformProcessorParameters(CalvalusProcessor calvalusProcessor) {
        ParameterDescriptor[] processorParameters = calvalusProcessor.getParameterDescriptors();
        StringBuilder sb = new StringBuilder();
        sb.append("<parameters>\n");
        if (processorParameters != null) {
            for (ParameterDescriptor parameterDescriptor : processorParameters) {
                String processorParameterValue = inputMapRaw.get(parameterDescriptor.getName());
                String defaultParameterValue = parameterDescriptor.getDefaultValue();
                if (StringUtils.isNotBlank(processorParameterValue) || StringUtils.isNotBlank(defaultParameterValue)) {
                    sb.append("<");
                    sb.append(parameterDescriptor.getName());
                    sb.append(">");

                    sb.append(StringUtils.isNotBlank(processorParameterValue) ? processorParameterValue : defaultParameterValue);

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
