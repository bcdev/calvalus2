package com.bc.calvalus.wps.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
public class ProductMetadata {

    private String jobFinishTime;
    private String productOutputDir;
    private String productionName;
    private String processName;
    private String inputDatasetName;
    private String regionWkt;
    private String regionBox;
    private String startDate;
    private String stopDate;
    private String collectionUrl;
    private String processorVersion;
    private String productionType;
    private String outputFormat;
    private List<Map> productList;


    public ProductMetadata(ProductMetadataBuilder builder) {
        createProductMetadata(builder);
    }

    public Map<String, Object> getContextMap() {
        Map<String, Object> contextMap = new HashMap<>();

        contextMap.put("jobFinishTime", jobFinishTime);
        contextMap.put("productOutputDir", productOutputDir);
        contextMap.put("productionName", productionName);
        contextMap.put("processName", processName);
        contextMap.put("inputDatasetName", inputDatasetName);
        contextMap.put("regionWkt", regionWkt);
        contextMap.put("regionBox", regionBox);
        contextMap.put("startDate", startDate);
        contextMap.put("stopDate", stopDate);
        contextMap.put("collectionUrl", collectionUrl);
        contextMap.put("processorVersion", processorVersion);
        contextMap.put("productionType", productionType);
        contextMap.put("outputFormat", outputFormat);
        contextMap.put("productList", productList);

        return contextMap;
    }

    private void createProductMetadata(ProductMetadataBuilder builder) {
        jobFinishTime = builder.getJobFinishTime();
        productOutputDir = builder.getProductOutputDir();
        productionName = builder.getProductionName();
        processName = builder.getProcessName();
        inputDatasetName = builder.getInputDatasetName();
        regionWkt = builder.getRegionWkt();
        regionBox = builder.getRegionBox();
        startDate = builder.getStartDate();
        stopDate = builder.getStopDate();
        collectionUrl = builder.getCollectionUrl();
        processorVersion = builder.getProcessorVersion();
        productionType = builder.getProductionType();
        outputFormat = builder.getOutputFormat();
        productList = builder.getProductList();
    }

}
