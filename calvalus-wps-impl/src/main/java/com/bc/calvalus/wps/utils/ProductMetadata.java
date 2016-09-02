package com.bc.calvalus.wps.utils;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs.DATE_FORMAT;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs.MAX_DATE;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs.MIN_DATE;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class ProductMetadata {

    private String jobUrl;
    private String jobFinishTime;
    private String productOutputDir;
    private String productionName;
    private String processName;
    private String inputDatasetName;
    private String stagingDir;
    private String regionWkt;
    private String startDate;
    private String stopDate;
    private String collectionUrl;
    private String processorVersion;
    private String productionType;
    private String outputFormat;
    private List<Map> productList;

    private final Production production;
    private final List<File> productionResults;
    private final WpsServerContext serverContext;

    private static final Logger LOG = CalvalusLogger.getLogger();

    public ProductMetadata(ProductMetadataBuilder builder) {
        this.production = builder.getProduction();
        this.productionResults = builder.getProductionResults();
        this.serverContext = builder.getServerContext();
        createProductMetadata(builder);
    }

    public Map<String, Object> getContextMap() {
        Map<String, Object> contextMap = new HashMap<>();

        contextMap.put("jobUrl", jobUrl);
        contextMap.put("jobFinishTime", jobFinishTime);
        contextMap.put("productOutputDir", productOutputDir);
        contextMap.put("productionName", productionName);
        contextMap.put("processName", processName);
        contextMap.put("inputDatasetName", inputDatasetName);
        contextMap.put("stagingDir", stagingDir);
        contextMap.put("regionWkt", regionWkt);
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
        jobUrl = builder.getJobUrl();
        jobFinishTime = builder.getJobFinishTime();
        productOutputDir = builder.getProductOutputDir();
        productionName = builder.getProductionName();
        processName = builder.getProcessName();
        inputDatasetName = builder.getInputDatasetName();
        stagingDir = builder.getStagingDir();
        regionWkt = builder.getRegionWkt();
        startDate = builder.getStartDate();
        stopDate = builder.getStopDate();
        collectionUrl = builder.getCollectionUrl();
        processorVersion = builder.getProcessorVersion();
        productionType = builder.getProductionType();
        outputFormat = builder.getOutputFormat();
        productList = builder.getProductList();
    }

    public String getJobUrl() {
        return jobUrl;
    }

    public String getJobFinishTime() {
        return jobFinishTime;
    }

    public String getProductOutputDir() {
        return productOutputDir;
    }

    public String getProductionName() {
        return productionName;
    }

    public String getProcessName() {
        return processName;
    }

    public String getInputDatasetName() {
        return inputDatasetName;
    }

    public String getStagingDir() {
        return stagingDir;
    }

    public String getRegionWkt() {
        return regionWkt;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getStopDate() {
        return stopDate;
    }

    public String getCollectionUrl() {
        return collectionUrl;
    }

    public String getProcessorVersion() {
        return processorVersion;
    }

    public String getProductionType() {
        return productionType;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public List<Map> getProductList() {
        return productList;
    }

}
