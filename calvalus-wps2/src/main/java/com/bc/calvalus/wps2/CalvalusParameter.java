package com.bc.calvalus.wps2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hans on 20/08/2015.
 */
public enum CalvalusParameter {

    PRODUCTION_TYPE("productionType", "Production type"),
    CALVALUS_BUNDLE_VERSION ("calvalus.calvalus.bundle", "Calvalus bundle version"),
    BEAM_BUNDLE_VERSION("calvalus.beam.bundle", "Beam bundle version"),
    PRODUCT_NAME("productionName", "Production name"),
    PROCESSOR_BUNDLE_NAME("processorBundleName", "Processor bundle name"),
    PROCESSOR_BUNDLE_VERSION("processorBundleVersion", "Processor bundle version"),
    PROCESSOR_NAME("processorName", "Processor name"),
    INPUT_PATH("inputPath", "Input path"),
    MIN_DATE("minDate", "Date from"),
    MAX_DATE("maxDate", "Date to"),
    PERIOD_LENGTH("periodLength", "Period length"),
    REGION_WKT("regionWkt", "Region WKT"),
    CALVALUS_OUTPUT_FORMAT("calvalus.output.format", "Calvalus output format");

    private String identifier;
    private String abstractText;

    CalvalusParameter(String identifier, String abstractText) {
        this.identifier = identifier;
        this.abstractText = abstractText;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getAbstractText() {
        return abstractText;
    }

    public List<String> getAllParameters(){
        List<String> allParameters = new ArrayList<>();
        for(CalvalusParameter calvalusParameter : CalvalusParameter.values()){
        }
    }
}
