package com.bc.calvalus.wps.calvalusfacade;

import java.util.ArrayList;
import java.util.List;

/**
 * A list of production parameters with their corresponding abstract
 * and title values (in WPS request context).
 *
 * @author hans
 */
public enum CalvalusParameter {

    PRODUCTION_TYPE("productionType", "Production type", "productionInfo"),
    CALVALUS_BUNDLE_VERSION("calvalus.calvalus.bundle", "Calvalus bundle version", "productionInfo"),
    BEAM_BUNDLE_VERSION("calvalus.beam.bundle", "Beam bundle version", "productionInfo"),
    SNAP_BUNDLE_VERSION("calvalus.snap.bundle", "Snap bundle version", "productionInfo"),
    PRODUCT_NAME("productionName", "Production name", "productionInfo"),

    PROCESSOR_BUNDLE_NAME("processorBundleName", "Processor bundle name", "processorInfo"),
    PROCESSOR_BUNDLE_VERSION("processorBundleVersion", "Processor bundle version", "processorInfo"),
    PROCESSOR_NAME("processorName", "Processor name", "processorInfo"),

    INPUT_DATASET("inputDataSetName", "Input dataset name", "inputDataSet"),

    MIN_DATE("minDate", "Date from", "productSet"),
    MAX_DATE("maxDate", "Date to", "productSet"),
    PERIOD_LENGTH("periodLength", "Period length", "productSet"),
    REGION_WKT("regionWKT", "Region WKT", "productSet"),
    CALVALUS_OUTPUT_FORMAT("outputFormat", "Calvalus output format", "productSet");

    private String identifier;
    private String abstractText;
    private String type;

    CalvalusParameter(String identifier, String abstractText, String type) {
        this.identifier = identifier;
        this.abstractText = abstractText;
        this.type = type;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getAbstractText() {
        return abstractText;
    }

    public String getType() {
        return type;
    }

    public static List<String> getAllParameters() {
        List<String> allParameters = new ArrayList<>();
        for (CalvalusParameter calvalusParameter : CalvalusParameter.values()) {
            allParameters.add(calvalusParameter.getIdentifier());
        }
        return allParameters;
    }

    public static List<String> getProductionInfoParameters() {
        List<String> productionParameters = new ArrayList<>();
        for (CalvalusParameter calvalusParameter : CalvalusParameter.values()) {
            if(calvalusParameter.getType().equals("productionInfo")){
                productionParameters.add(calvalusParameter.getIdentifier());
            }
        }
        return productionParameters;
    }

    public static List<String> getProductsetParameters() {
        List<String> productionParameters = new ArrayList<>();
        for (CalvalusParameter calvalusParameter : CalvalusParameter.values()) {
            if(calvalusParameter.getType().equals("productSet")){
                productionParameters.add(calvalusParameter.getIdentifier());
            }
        }
        return productionParameters;
    }

    public static List<String> getProcessorInfoParameters() {
        List<String> productionParameters = new ArrayList<>();
        for (CalvalusParameter calvalusParameter : CalvalusParameter.values()) {
            if(calvalusParameter.getType().equals("processorInfo")){
                productionParameters.add(calvalusParameter.getIdentifier());
            }
        }
        return productionParameters;
    }
}
