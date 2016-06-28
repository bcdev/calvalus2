package com.bc.calvalus.wps.utils;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
import org.apache.velocity.VelocityContext;

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

    public ProductMetadata(Production production, List<File> productionResults, WpsServerContext serverContext)
                throws ProductionException {
        this.production = production;
        this.productionResults = productionResults;
        this.serverContext = serverContext;
        createProductMetadata();
    }

    public VelocityContext createVelocityContext() {
        VelocityContext context = new VelocityContext();

        context.put("jobUrl", jobUrl);
        context.put("jobFinishTime", jobFinishTime);
        context.put("productOutputDir", productOutputDir);
        context.put("productionName", productionName);
        context.put("processName", processName);
        context.put("inputDatasetName", inputDatasetName);
        context.put("stagingDir", stagingDir);
        context.put("regionWkt", regionWkt);
        context.put("startDate", startDate);
        context.put("stopDate", stopDate);
        context.put("collectionUrl", collectionUrl);
        context.put("processorVersion", processorVersion);
        context.put("productionType", productionType);
        context.put("outputFormat", outputFormat);
        context.put("productList", productList);

        return context;
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

    private void createProductMetadata() throws ProductionException {
        ProductionRequest productionRequest = production.getProductionRequest();

        jobUrl = serverContext.getRequestUrl();
        jobFinishTime = getDateInXmlGregorianCalendarFormat(production.getWorkflow().getStopTime()).toString();
        String stagingPath = productionRequest.getStagingDirectory(production.getId());
        productOutputDir = production.getName() + "/" + stagingPath;
        productionName = production.getName();
        processName = productionRequest.getString(PROCESSOR_NAME.getIdentifier());
        inputDatasetName = productionRequest.getString("inputPath");
        stagingDir = getBaseStagingUrl() + "/" + stagingPath.split("/")[0];
        regionWkt = extractRegionWkt(productionRequest.getString(("regionWKT")));
        startDate = "DUMMY";
        stopDate = "DUMMY";
        collectionUrl = getBaseStagingUrl() + "/" + production.getStagingPath();
        processorVersion = productionRequest.getString("processorBundleVersion");
        productionType = productionRequest.getString("productionType");
        outputFormat = productionRequest.getString("outputFormat");
        createProductList();
    }

    private String extractRegionWkt(String regionWkt) {
        return regionWkt.replaceAll("POLYGON\\(\\(", "").replaceAll("\\)\\)", "").replace(",", " ");
    }

    private XMLGregorianCalendar getDateInXmlGregorianCalendarFormat(Date date) {
        GregorianCalendar c = new GregorianCalendar();
        c.setTime(date);
        XMLGregorianCalendar date2 = null;
        try {
            date2 = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
        } catch (DatatypeConfigurationException e) {
            e.printStackTrace();
        }
        return date2;
    }

    private void createProductList() {
        productList = new ArrayList<>();
        for (File productionResult : productionResults) {
            Map<String, String> productMap = new HashMap<>();
            productMap.put("productUrl", getBaseStagingUrl() + "/" + productionResult.getName());
            productMap.put("productFileName", productionResult.getName());
            productMap.put("productFileFormat", parseFileFormat(productionResult.getName()));
            productMap.put("productFileSize", Long.toString(productionResult.length()));
            productMap.put("productQuickLookUrl", getBaseStagingUrl() + "/" + "QUICKLOOK");
            productList.add(productMap);
        }
    }

    private String parseFileFormat(String fileName) {
        if (fileName.toLowerCase().endsWith(".zip")) {
            return "ZIP";
        } else if (fileName.toLowerCase().endsWith(".xml")) {
            return "XML";
        } else if (fileName.toLowerCase().endsWith("-metadata")) {
            return "metadata";
        } else {
            return outputFormat;
        }
    }

    private String getBaseStagingUrl() {
        return "http://"
               + serverContext.getHostAddress()
               + ":" + serverContext.getPort()
               + "/" + PropertiesWrapper.get("wps.application.name")
               + "/" + PropertiesWrapper.get("staging.directory");
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
