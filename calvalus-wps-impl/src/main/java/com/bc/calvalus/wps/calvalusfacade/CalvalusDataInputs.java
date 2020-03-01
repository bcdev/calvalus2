package com.bc.calvalus.wps.calvalusfacade;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import com.bc.calvalus.processing.ra.RAConfig;
import com.bc.calvalus.processing.ra.RARegions;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.ceres.binding.BindingException;
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
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.*;

/**
 * This class transform the input parameters map into a format recognized by Calvalus Production Request.
 *
 * @author hans
 */
public class CalvalusDataInputs {

    public static final long MIN_DATE = 1451606400000L;
    public static final long MAX_DATE = 1483228800000L;
    public static SimpleDateFormat ISO_DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");

    private final Map<String, String> inputMapRaw;
    private final Map<String, String> inputMapFormatted;

    CalvalusDataInputs(ExecuteRequestExtractor executeRequestExtractor,
                       WpsProcess calvalusProcessor,
                       ProductSet[] productSets,
                       CalvalusFacade calvalusFacade)
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
        } else if (extractRAParamters(calvalusProcessor, calvalusFacade)) {
            inputMapFormatted.put("productionType", "RA");
            String productionName = executeRequestExtractor.getValue("productionName");
            if (productionName != null) {
                inputMapFormatted.put("calvalus.output.prefix", productionName.replaceAll(" ", "_"));
            }
        } else {
            inputMapFormatted.put("productionType", "L2Plus");
            // TODO check whether this is safe
            inputMapFormatted.put("calvalus.system.snap.dataio.bigtiff.support.pushprocessing", "false");
            // determine and set period length - dates are expected in the geo index impl, null is not supported
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
        }
        if (inputMapRaw.containsKey("calvalus.ql.parameters")) {
            inputMapFormatted.put("calvalus.ql.parameters", inputMapRaw.get("calvalus.ql.parameters"));
            CalvalusLogger.getLogger().info("ql parameters found in request:" + inputMapRaw.get("calvalus.ql.parameters"));
        } else if (calvalusProcessor.getJobConfiguration().containsKey("calvalus.ql.parameters")) {
            inputMapFormatted.put("calvalus.ql.parameters", calvalusProcessor.getJobConfiguration().get("calvalus.ql.parameters"));
        }
        inputMapFormatted.put("autoStaging", "true");
        inputMapFormatted.put("calvalus.output.compression", "none");
        if(calvalusFacade.getRequestHeaderMap().get("remoteUser") != null){
            inputMapFormatted.put("calvalus.wps.remote.user", calvalusFacade.getRequestHeaderMap().get("remoteUser"));
        }
        if(calvalusFacade.getRequestHeaderMap().get("remoteRef") != null){
            inputMapFormatted.put("calvalus.wps.remote.ref", calvalusFacade.getRequestHeaderMap().get("remoteRef"));
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
            } else if (requestParameters == null || requestParameters.trim().length() == 0) {
                if (! aggregate) {
                    return false;
                }
            } else {
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

    /**
     * Identifies RA request and converts WPS parameters into a Calvalus request.
     * Implicit input is inputMapRaw with the WPS request parameters.
     *     shapefile: Shape file name with features to be used for statistics, e.g. LUX_adm
     *     regionAttributeName: Shape file attribute for features to be used for statistics, e.g. NAME_3, leave empty for heuristic selection
     *     regionAttributeValueFilter: Pattern to select attribute values, .e.g. Lux*|Esch*, leave empty to select all regions of attribute
     *     bands: Bands of selected input to be used for statistics, defaults to non-default bands of regionalStatisticsParameters
     *     validPixelExpression: Band maths expression to identify pixels that shall be counted
     *     regionalStatisticsParameters: <parameters>
     *         <bands>
     *           <band><name>band_1</name><numBins>2</numBins><min>0</min><max>255</max></band>
     *           <band><name>ndvi_max</name><numBins>8</numBins><min>0</min><max>0.8</max></band>
     *           <band><name>ndwi_mean</name><numBins>8</numBins><min>0</min><max>0.8</max></band>
     *           <band><name>*</name><numBins>10</numBins><min>0</min><max>1</max></band>
     *         </bands>
     *         <percentiles>10,90</percentiles>
     *       </parameters>
     * Implicit output is inputMapFormatted with the added Calvalus request parameters.
     *     collectionName: DLR WSF 2015 (Urban TEP)
     *     geoInventory: /calvalus/geoInventory/WSF2015_v1_EPSG4326
     *     inputProductType: URBAN_FOOTPRINT
     *     regionWKT: POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))
     *     minDate: 2015-01-01
     *     maxDate: 2015-12-31
     *     periodLength: 365
     *     autoStaging: true
     *     outputFormat: Report
     *     calvalus.ra.binValuesAsRatio: true
     *     cavalus.ra.parameters: <parameters>
     *         <regions>
     *             <region><name>...</name><wkt>POLYGON((...))</wkt></region>
     *         </regions>
     *         <regionSource>http://.../region_data/....zip</regionSource>
     *         <regionSourceAttributeName>NAME_3</regionSourceAttributeName>
     *         <regionSourceAttributeFilter></regionSourceAttributeFilter>
     *         <goodPixelExpression>true</goodPixelExpression>
     *         <percentiles>90</percentiles>
     *         <bands>
     *             <band><name>band_1</name><numBins>2</numBins><min>0</min><max>255</max></band>
     *         </bands>
     *         <writePerRegion>false</writePerRegion>
     *         <writeSeparateHistogram>false</writeSeparateHistogram>
     *       </parameters>
     * @param processor  processor descriptor from bundle descriptor, with default regionalStatistics configuration, e.g.
     *     <regionalStatisticsParameters>
     *       <parameters>
     *         <bands>
     *           <band><name>band_1</name><numBins>2</numBins><min>0</min><max>255</max></band>
     *           <band><name>ndvi_max</name><numBins>8</numBins><min>0</min><max>0.8</max></band>
     *           <band><name>*</name><numBins>10</numBins><min>0</min><max>1</max></band>
     *         </bands>
     *         <percentiles>90</percentiles>
     *       </parameters>
     *     </regionalStatisticsParameters>
     * @param calvalusFacade access to shapefiles
     * @return whether the bundle descriptor contains the RA parameters
     * @throws InvalidParameterValueException
     */
    private boolean extractRAParamters(WpsProcess processor, CalvalusFacade calvalusFacade) throws InvalidParameterValueException {
        String regionalStatisticsParameters = inputMapRaw.get("regionalStatisticsParameters");
        if (regionalStatisticsParameters == null) {
            return false;
        }
        // remove dummy processor "ra" and parameters
        if ("ra".equals(inputMapFormatted.get("processorName"))) {
            inputMapFormatted.remove("processorName");
            inputMapFormatted.remove("processorParameters");
        }
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
        // accumulate ra parameters
        StringBuilder accu = new StringBuilder();
        accu.append("<parameters>");
        // read shape file if specified
        String regionSource = null;
        String shapefile = inputMapRaw.get("shapefile");
        if (shapefile != null && shapefile.length() > 0) {
            String[] regionFiles;
            try {
                regionFiles = calvalusFacade.getRegionFiles();
            } catch (IOException e) {
                throw new InvalidParameterValueException(e, "shapefiles of user not found");
            }
            for (String path : regionFiles) {
                String filename = path.substring(path.lastIndexOf('/') + 1);
                if (filename.equals(shapefile) || filename.equals(shapefile + ".zip")) {
                    regionSource = path;
                    break;
                }
            }
            if (regionSource == null) {
                throw new InvalidParameterValueException("shapefile " + shapefile + " not found among " + String.join(" ", regionFiles));
            }
            accu.append("<regionSource>");
            accu.append(regionSource);
            accu.append("</regionSource>");
            String regionAttributeName = inputMapRaw.get("regionAttributeName");
            if (regionAttributeName == null) {
                regionAttributeName = inputMapRaw.get("regionSourceAttributeName");
            }
            if (regionAttributeName == null || regionAttributeName.length() == 0) {
                String[][] regionAttributeValues;
                try {
                    regionAttributeValues = calvalusFacade.loadRegionDataInfo(regionSource);
                } catch (IOException | ProductionException e) {
                    throw new InvalidParameterValueException(e, "shapefile " + shapefile + " of user not found");
                }
                regionAttributeName = determineMostDistinctiveAttribute(regionAttributeValues);
            }
            accu.append("<regionSourceAttributeName>");
            accu.append(regionAttributeName);
            accu.append("</regionSourceAttributeName>");
            String regionAttributeValueFilter = inputMapRaw.get("regionAttributeValueFilter");
            if (regionAttributeValueFilter == null) {
                regionAttributeValueFilter = inputMapRaw.get("regionSourceAttributeFilter");
            }
            if (regionAttributeValueFilter != null && regionAttributeValueFilter.length() != 0) {
                accu.append("<regionSourceAttributeFilter>");
                accu.append(regionAttributeValueFilter);
                accu.append("</regionSourceAttributeFilter>");
            }
        } else {
            accu.append("<regions><region><name>");
            accu.append(inputMapRaw.getOrDefault("regionName", inputMapRaw.getOrDefault("productionName", "selected_region")));
            accu.append("</name><wkt>");
            accu.append(inputMapRaw.get("regionWkt"));
            accu.append("</wkt></region></regions>");
        }
        RAConfig raConfig;
        try {
            raConfig = RAConfig.fromXml(regionalStatisticsParameters);
        } catch (BindingException e) {
            throw new InvalidParameterValueException("failed to parse RA config from bundle descriptor");
        }
        accu.append("<goodPixelExpression>");
        accu.append(inputMapRaw.getOrDefault("validPixelExpression", "true"));
        accu.append("</goodPixelExpression>");
        if (raConfig.getPercentiles() != null && raConfig.getPercentiles().length > 0) {
            accu.append("<percentiles>");
            accu.append(raConfig.getPercentiles()[0]);
            for (int i=1; i<raConfig.getPercentiles().length; ++i) {
                accu.append(",");
                accu.append(raConfig.getPercentiles()[i]);
            }
            accu.append("</percentiles>");
        }
        String[] bandNames;
        String bandsString = inputMapRaw.get("bands");
        if (bandsString == null || bandsString.length() == 0) {
            bandNames = raConfig.getBandNames();
        } else {
            bandNames = bandsString.split(",");
            for (int i=0; i<bandNames.length; ++i) {
                bandNames[i] = bandNames[i].trim();
            }
        }
        accu.append("<bands>");
        for (String bandName : bandNames) {
            RAConfig.BandConfig bandConfig = findBandConfig(bandName, raConfig);
            accu.append("<band><name>");
            accu.append(bandName);
            accu.append("</name><numBins>");
            accu.append(bandConfig.getNumBins());
            accu.append("</numBins><min>");
            accu.append(bandConfig.getMin());
            accu.append("</min><max>");
            accu.append(bandConfig.getMax());
            accu.append("</max></band>");
        }
        accu.append("</bands>");
        accu.append("<writePerRegion>false</writePerRegion><writeSeparateHistogram>false</writeSeparateHistogram></parameters>");
        inputMapFormatted.put("calvalus.ra.parameters", accu.toString());
        inputMapFormatted.put("calvalus.ra.binValuesAsRatio", String.valueOf(raConfig.isBinValuesAsRatio()));
        return true;
    }

    private RAConfig.BandConfig findBandConfig(String bandName, RAConfig raConfig) {
        for (RAConfig.BandConfig bandConfig : raConfig.getBandConfigs()) {
            if (bandName.matches(bandConfig.getName())) {
                return bandConfig;
            }
        }
        return new RAConfig.BandConfig(".*", 10, 0.0, 1.0);
    }

    static String determineMostDistinctiveAttribute(String[][] regionAttributeValues) throws InvalidParameterValueException {
        if (regionAttributeValues.length == 0) {
            throw new InvalidParameterValueException("no attributes found in shapefile");
        }
        int bestIndex = 0;
        int bestSize = 0;
        for (int i=0; i<regionAttributeValues[0].length; ++i) {
            int size = getSizeOf(regionAttributeValues, i);
            if (size > bestSize) {
                bestIndex = i;
                bestSize = size;
            }
        }
        // TODO: allow for non-distinctive attributes in RA reducer
        return regionAttributeValues[0][bestIndex];
    }

    static int getSizeOf(String[][] regionAttributeValues, int attributeIndex) {
        int size = 0;
        for (int i=1; i<regionAttributeValues.length; ++i) {
            for (int j=1;; ++j) {
                if (j >= i) {
                    ++size;
                    break;
                }
                if (regionAttributeValues[i][attributeIndex].equals(regionAttributeValues[j][attributeIndex])) {
                    break;
                }
            }
        }
        return size;
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
                putBands(productSet);
                break;
            }
        }

        if ((calvalusProcessor != null)
            && StringUtils.isBlank(inputMapFormatted.get("inputPath"))
            && StringUtils.isBlank(inputMapFormatted.get("geoInventory"))) {
            throw new InvalidParameterValueException(INPUT_DATASET.getIdentifier());
        }
    }

    private void putBands(ProductSet productSet) {
        String[] bandNames = productSet.getBandNames();
        if (inputMapRaw.get("bands") == null && bandNames != null && bandNames.length != 0) {
            inputMapRaw.put("bands", String.join(",", bandNames));
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
                if (productSetPath.contains(":")) {
                    // s3a://urbantep/calvalus/...
                    inputPathStringBuilder.append(productSetPath).append(",");
                } else {
                    // eodata/...
                    inputPathStringBuilder.append("/calvalus/").append(productSetPath).append(",");
                }
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
                if ("ra".equals(inputMapFormatted.get("processorName"))
                        && ("shapefile".equals(parameterDescriptor.getName())
                        || "bands".equals(parameterDescriptor.getName())
                        || "validPixelExpression".equals(parameterDescriptor.getName()))) {
                    continue;
                }
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
