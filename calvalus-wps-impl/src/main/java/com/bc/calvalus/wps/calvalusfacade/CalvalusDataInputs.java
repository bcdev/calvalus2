package com.bc.calvalus.wps.calvalusfacade;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.*;

/**
 * This class transform the input parameters map into a format recognized by Calvalus Production Request.
 *
 * @author hans
 */
public class CalvalusDataInputs {

    public static final long MIN_DATE = 1451606400000L;
    public static final long MAX_DATE = 1483228800000L;
    public static SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
//    static {
//        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
//    }

    private final Map<String, String> inputMapRaw;
    private final Map<String, String> inputMapFormatted;

    CalvalusDataInputs(ExecuteRequestExtractor executeRequestExtractor,
                       WpsProcess calvalusProcessor,
                       ProductSet[] productSets,
                       Map<String, String> requestHeaderMap)
                throws InvalidParameterValueException {
        inputMapFormatted = new HashMap<>();
        inputMapRaw = executeRequestExtractor.getInputParametersMapRaw();
        for (String key : calvalusProcessor.getJobConfiguration().keySet()) {
            CalvalusLogger.getLogger().info("job config " + key + " = " + calvalusProcessor.getJobConfiguration().get(key));
        }
        extractProductionParameters();
        if (calvalusProcessor != null) {
            extractProductionInfoParameters((CalvalusProcessor) calvalusProcessor);
            extractProcessorInfoParameters((CalvalusProcessor) calvalusProcessor);
            transformProcessorParameters((CalvalusProcessor) calvalusProcessor);
        }
        extractProductSetParameters(productSets, (CalvalusProcessor) calvalusProcessor);
        if (extractL3Parameters(calvalusProcessor)) {
            inputMapFormatted.put("productionType", "L3");
            String productionName = executeRequestExtractor.getValue("productionName");
            if (productionName != null) {
                inputMapFormatted.put("calvalus.output.prefix", productionName.replaceAll(" ", "_"));
            }
        } else {
            inputMapFormatted.put("productionType", "L2Plus");
        }
        if (inputMapRaw.containsKey("calvalus.ql.parameters")) {
            inputMapFormatted.put("calvalus.ql.parameters", inputMapRaw.get("calvalus.ql.parameters"));
            CalvalusLogger.getLogger().info("ql parameters found in request:" + inputMapRaw.get("calvalus.ql.parameters"));
        } else if (calvalusProcessor.getJobConfiguration().containsKey("calvalus.ql.parameters")) {
            inputMapFormatted.put("calvalus.ql.parameters", calvalusProcessor.getJobConfiguration().get("calvalus.ql.parameters"));
        }
        inputMapFormatted.put("autoStaging", "true");
        inputMapFormatted.put("calvalus.output.compression", "none");
        if(requestHeaderMap.get("remoteUser") != null){
            inputMapFormatted.put("calvalus.wps.remote.user", requestHeaderMap.get("remoteUser"));
        }
        if(requestHeaderMap.get("remoteRef") != null){
            inputMapFormatted.put("calvalus.wps.remote.ref", requestHeaderMap.get("remoteRef"));
        }
        inputMapFormatted.put("quicklooks", "true");
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
    Map<String, String> getInputMapFormatted() {
        return inputMapFormatted;
    }

    private boolean extractL3Parameters(WpsProcess processor) throws InvalidParameterValueException {
// MB, 2017-04-16: merge spatioTemporalAggregation parameters from request and processor descriptor
//        if (StringUtils.isNotBlank(inputMapRaw.get("calvalus.l3.parameters"))) {
//            inputMapFormatted.put("calvalus.l3.parameters", inputMapRaw.get("calvalus.l3.parameters"));
//        }

//        <spatioTemporalAggregationParameters>
//          <aggregate>true</aggregate>
//          <spatialResolution>60</spatialResolution>
//          <spatialRule>NEAREST</spatialRule>
//          <temporalRules>AVG,MIN_MAX</temporalRules>
//          <variables>ndbi,ndvi,ndwi</variables>
//          <validPixelExpression>not pixel_classif_flags.CLOUD and not pixel_classif_flags.INVALID</validPixelExpression>
//        </spatioTemporalAggregationParameters>

        DocumentBuilder xbuilder;
        try {
            xbuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new InvalidParameterValueException(e, "spatioTemporalAggregationParameters");
        }
        XPath xpath = XPathFactory.newInstance().newXPath();
        String defaultParameters = processor.getJobConfiguration().get("calvalus.wps.spatioTemporalAggregation");
        if (defaultParameters == null) {
            return false;
        }
        // parse default parameters
        boolean aggregate;
        int spatialResolution;
        String spatialRule;
        String[] temporalRules;
        String[] variables;
        String validPixelExpression;
        try {
            Document defaultDoc = xbuilder.parse(new InputSource(new StringReader(defaultParameters)));
            aggregate = Boolean.parseBoolean(xpath.evaluate("/spatioTemporalAggregationParameters/aggregate", defaultDoc));
            spatialResolution = Integer.parseInt(xpath.evaluate("/spatioTemporalAggregationParameters/spatialResolution", defaultDoc));
            spatialRule = xpath.evaluate("/spatioTemporalAggregationParameters/spatialRule", defaultDoc);
            temporalRules = xpath.evaluate("/spatioTemporalAggregationParameters/temporalRules", defaultDoc).split(",");
            variables = xpath.evaluate("/spatioTemporalAggregationParameters/variables", defaultDoc).split(",");
            validPixelExpression = xpath.evaluate("/spatioTemporalAggregationParameters/validPixelExpression", defaultDoc);
        } catch (Exception e) {
            throw new InvalidParameterValueException(e.getMessage(), e, "processorDescriptor job config spatioTemporalAggregation");
        }
        // distinguish requestParameters values "true", "false", or XML
        try {
            String requestParameters = inputMapRaw.get("spatioTemporalAggregationParameters");
            if ("false".equals(requestParameters)) {
                return false;
            } else if ("true".equals(requestParameters)) {
                aggregate = true;
            } else if (requestParameters != null && requestParameters.trim().length() > 0 && !requestParameters.trim().startsWith("<")) {
                aggregate = true;
                spatialResolution = Integer.parseInt(requestParameters.trim());
            } else if (requestParameters != null && requestParameters.trim().length() > 0) {
                Document requestDoc = xbuilder.parse(new InputSource(new StringReader(requestParameters)));
                String value = xpath.evaluate("/spatioTemporalAggregationParameters/aggregate", requestDoc);
                if (value != null) {
                    aggregate = Boolean.parseBoolean(value);
                }
                if (!aggregate) {
                    return false;
                }
                value = xpath.evaluate("/spatioTemporalAggregationParameters/spatialResolution", requestDoc);
                if (value != null) {
                    spatialResolution = Integer.parseInt(value);
                }
                value = xpath.evaluate("/spatioTemporalAggregationParameters/spatialRule", requestDoc);
                if (value != null) {
                    spatialRule = value;
                }
                value = xpath.evaluate("/spatioTemporalAggregationParameters/temporalRules", requestDoc);
                if (value != null) {
                    temporalRules = value.split(",");
                }
                value = xpath.evaluate("/spatioTemporalAggregationParameters/variables", requestDoc);
                if (value != null) {
                    variables = value.split(",");
                }
                value = xpath.evaluate("/spatioTemporalAggregationParameters/validPixelExpression", requestDoc);
                if (value != null) {
                    validPixelExpression = value;
                }
            }
        } catch (Exception e) {
            throw new InvalidParameterValueException(e.getMessage(), e, "spatioTemporalAggregationParameters");
        }
        // construct calvalus.l3.parameters
        StringBuilder accu = new StringBuilder();
        //accu.append("<wps:Data><wps:ComplexData><parameters><compositingType>");
        accu.append("<parameters><compositingType>");
        if ("NEAREST".equals(spatialRule)) {
            accu.append("MOSAICKING");
        } else if ("BINNING".equals(spatialRule)) {
            accu.append("BINNING");
        } else {
            throw new IllegalArgumentException("unknown spatialRule " + spatialRule + ". NEAREST or BINNING expected.");
        }
        accu.append("</compositingType><planetaryGrid>org.esa.snap.binning.support.PlateCarreeGrid</planetaryGrid><numRows>");
        accu.append(String.valueOf(19440000 / spatialResolution));
        accu.append("</numRows><superSampling>1</superSampling><maskExpr>");
        accu.append(validPixelExpression);
        accu.append("</maskExpr><aggregators>");
        for (String aggregator : temporalRules) {
            if (aggregator.startsWith("ON_MAX_SET")) {
                accu.append("<aggregator><type>ON_MAX_SET</type><onMaxVarName>");
                accu.append(aggregator.substring("ON_MAX_SET(".length(), aggregator.length()-1));
                accu.append("</onMaxVarName><setVarNames>");
                accu.append(String.join(",", variables));
                accu.append("</setVarNames></aggregator>");
            } else {
                for (String variable : variables) {
                    if (aggregator.startsWith("PERCENTILE")) {
                        accu.append("<aggregator><type>PERCENTILE</type><percentage>");
                        accu.append(aggregator.substring("PERCENTILE(".length(), aggregator.length() - 1));
                        accu.append("</percentage><varName>");
                        accu.append(variable);
                        accu.append("</varName></aggregator>");
                    } else {
                        accu.append("<aggregator><type>");
                        accu.append(aggregator);
                        accu.append("</type><varName>");
                        accu.append(variable);
                        accu.append("</varName></aggregator>");
                    }
                }
            }
        }
        //accu.append("</aggregators></parameters></wps:ComplexData></wps:Data>");
        accu.append("</aggregators></parameters>");
        inputMapFormatted.put("calvalus.l3.parameters", accu.toString());
        // determine and set period length
        String minDate = inputMapRaw.get("minDate");
        if (minDate == null || minDate.length() == 0) {
            minDate = inputMapFormatted.get("minDateSource");
            inputMapFormatted.put("minDate", minDate);
        }
        String maxDate = inputMapRaw.get("maxDate");
        if (maxDate == null || maxDate.length() == 0) {
            maxDate = inputMapFormatted.get("maxDateSource");
            inputMapFormatted.put("maxDate", maxDate);
        }
        try {
            long millis = ISO_DATE_FORMAT.parse(maxDate).getTime() - ISO_DATE_FORMAT.parse(minDate).getTime();
            int periodLength = (int) ((millis + (1000 * 60 * 60 * 24) * 3 / 2) / (1000 * 60 * 60 * 24));
            inputMapFormatted.put("periodLength", String.valueOf(periodLength));
        } catch (ParseException e) {
            throw new InvalidParameterValueException("failed to parse date " + minDate + " or " + maxDate, e, "minDate or maxDate or dataset temporal coverage");
        }
        return true;
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
                putProductPath(productSet);
                putDates(productSet);
                break;
            }
        }

        if ((calvalusProcessor != null)
            && StringUtils.isBlank(inputMapFormatted.get("inputPath"))
            && StringUtils.isBlank(inputMapFormatted.get("geoInventory"))) {
            throw new InvalidParameterValueException(INPUT_DATASET.getIdentifier());
        }
    }

    private void putDates(ProductSet productSet) {
        // TBD: change to UTC
        ZonedDateTime defaultMinDate = ZonedDateTime.ofInstant(new Date(MIN_DATE).toInstant(), ZoneId.systemDefault());
        ZonedDateTime defaultMaxDate = ZonedDateTime.ofInstant(new Date(MAX_DATE).toInstant(), ZoneId.systemDefault());
        ZonedDateTime minDate = productSet.getMinDate() == null ? defaultMinDate : ZonedDateTime.ofInstant(productSet.getMinDate().toInstant(), ZoneId.systemDefault());
        ZonedDateTime maxDate = productSet.getMaxDate() == null ? defaultMaxDate : ZonedDateTime.ofInstant(productSet.getMaxDate().toInstant(), ZoneId.systemDefault());
        inputMapFormatted.putIfAbsent("minDateSource", minDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        inputMapFormatted.putIfAbsent("maxDateSource", maxDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
    }

    private void putProductPath(ProductSet productSet) {
        if (StringUtils.isNotBlank(productSet.getGeoInventory())) {
            inputMapFormatted.put(INPUT_DATASET_GEODB.getIdentifier(), productSet.getGeoInventory());
        } else {
            StringBuilder inputPathStringBuilder = new StringBuilder();
            for (String productSetPath : productSet.getPath().split(",")) {
                inputPathStringBuilder.append("/calvalus/").append(productSetPath).append(",");
            }
            String inputPathString = inputPathStringBuilder.substring(0, inputPathStringBuilder.length() - 1);
            inputMapFormatted.put("inputPath", inputPathString);
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
