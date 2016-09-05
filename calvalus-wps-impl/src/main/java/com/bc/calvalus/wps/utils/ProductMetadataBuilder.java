package com.bc.calvalus.wps.utils;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs.DATE_FORMAT;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs.MAX_DATE;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs.MIN_DATE;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wps.calvalusfacade.IWpsProcess;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
public class ProductMetadataBuilder {

    private String jobUrl;
    private String jobFinishTime;
    private String productOutputDir;
    private String productionName;
    private String processName;
    private String inputDatasetName;
    private String stagingDir;
    private String regionWkt;
    private String regionBox;
    private String startDate;
    private String stopDate;
    private String collectionUrl;
    private String processorVersion;
    private String productionType;
    private String outputFormat;
    private List<Map> productList;

    private boolean isLocal;
    private Production production;
    private List<File> productionResults;
    private WpsServerContext serverContext;
    private Map<String, Object> processParameters;
    private IWpsProcess processor;
    private String hostName;
    private int portNumber;
    private static final Logger LOG = CalvalusLogger.getLogger();


    public ProductMetadataBuilder() {
        this.isLocal = false;
    }

    public static ProductMetadataBuilder create() {
        return new ProductMetadataBuilder();
    }

    public ProductMetadataBuilder isLocal() {
        isLocal = true;
        return this;
    }

    public ProductMetadataBuilder withProduction(Production production) {
        this.production = production;
        return this;
    }

    public ProductMetadataBuilder withProductionResults(List<File> productionResults) {
        this.productionResults = productionResults;
        return this;
    }

    public ProductMetadataBuilder withServerContext(WpsServerContext serverContext) {
        this.serverContext = serverContext;
        this.hostName = serverContext.getHostAddress();
        this.portNumber = serverContext.getPort();
        return this;
    }

    public ProductMetadataBuilder withProcessParameters(Map<String, Object> processParameters) {
        this.processParameters = processParameters;
        return this;
    }

    public ProductMetadataBuilder withProductOutputDir(String productOutputDir) {
        this.productOutputDir = productOutputDir;
        return this;
    }

    public ProductMetadataBuilder withProcessor(IWpsProcess processor) {
        this.processor = processor;
        return this;
    }

    public ProductMetadataBuilder withHostName(String hostName) {
        this.hostName = hostName;
        return this;
    }

    public ProductMetadataBuilder withPortNumber(int portNumber) {
        this.portNumber = portNumber;
        return this;
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

    public String getRegionBox() {
        return regionBox;
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

    public Production getProduction() {
        return production;
    }

    public List<File> getProductionResults() {
        return productionResults;
    }

    public WpsServerContext getServerContext() {
        return serverContext;
    }

    public ProductMetadata build() throws ProductMetadataException {
        if (!isLocal) {
            ProductionRequest productionRequest = this.production.getProductionRequest();

            this.jobUrl = serverContext.getRequestUrl();
            this.jobFinishTime = getDateInXmlGregorianCalendarFormat(this.production.getWorkflow().getStopTime()).toString();
            String stagingPath = productionRequest.getStagingDirectory(this.production.getId());
            this.productOutputDir = this.production.getName() + "/" + stagingPath;
            this.productionName = this.production.getName();
            this.stagingDir = getBaseStagingUrl() + "/" + stagingPath.split("/")[0];
            this.collectionUrl = getBaseStagingUrl() + "/" + this.production.getStagingPath();
            try {
                this.processName = productionRequest.getString(PROCESSOR_NAME.getIdentifier());
                this.inputDatasetName = productionRequest.getString("inputDataSetName");
                String regionWktRaw = productionRequest.getString(("regionWKT"));
                this.regionWkt = extractRegionWkt(regionWktRaw);
                this.regionBox = parseRegionBox();
                this.startDate = getStartDate(productionRequest);
                this.stopDate = getStopDate(productionRequest);
                this.processorVersion = productionRequest.getString("processorBundleVersion");
                this.productionType = productionRequest.getString("productionType");
                this.outputFormat = productionRequest.getString("outputFormat");
            } catch (ProductionException exception) {
                throw new ProductMetadataException("Unable to create product metadata", exception);
            }
            this.productList = createProductList();
        } else {
            this.jobUrl = "anyUrl";
            this.jobFinishTime = getDateInXmlGregorianCalendarFormat(new Date()).toString();
            this.productionName = (String) processParameters.get("productionName");
            this.stagingDir = getBaseStagingUrl() + "/" + productOutputDir.split("/")[0] + "/";
            this.collectionUrl = getBaseStagingUrl() + "/" + productOutputDir + "/";
            this.processName = processor.getIdentifier().split("~")[2];
            this.inputDatasetName = (String) processParameters.get("sourceProduct");
            String regionWktRaw = (String) processParameters.get("geoRegion");
            this.regionWkt = extractRegionWkt(regionWktRaw);
            this.regionBox = parseRegionBox();
            this.startDate = DATE_FORMAT.format(MIN_DATE);
            this.stopDate = DATE_FORMAT.format(MAX_DATE);
            this.processorVersion = processor.getVersion();
            this.productionType = (String) processParameters.get("productionType");
            this.outputFormat = (String) processParameters.get("outputFormat");
        }
        return new ProductMetadata(this);
    }

    private String extractRegionWkt(String regionWkt) {
        return regionWkt.replaceAll("POLYGON\\(\\(", "").replaceAll("\\)\\)", "").replace(",", " ");
    }

    public String parseRegionBox() {
        String[] region = this.regionWkt.split(" ");
        List<Double> longitudes = new ArrayList<>();
        List<Double> latitudes = new ArrayList<>();
        try {
            for (int i = 0; i < region.length; i += 2) {
                longitudes.add(Double.valueOf(region[i]));
                latitudes.add(Double.valueOf(region[i + 1]));
            }
        } catch (NumberFormatException exception) {
            LOG.log(Level.WARNING, "Error in parsing the regionBox value.", exception);
            return this.regionWkt;
        }
        return String.format("%1$.5f %2$.5f %3$.5f %4$.5f",
                             Collections.min(latitudes),
                             Collections.min(longitudes),
                             Collections.max(latitudes),
                             Collections.max(longitudes));
    }

    private String getStartDate(ProductionRequest productionRequest) throws ProductionException {
        try {
            return DATE_FORMAT.format(productionRequest.createFromMinMax().getStartDate());
        } catch (ProductionException exception) {
            if (productionRequest.getString("minDateSource") != null) {
                return productionRequest.getString("minDateSource");
            } else {
                return DATE_FORMAT.format(MIN_DATE);
            }
        }
    }

    private String getStopDate(ProductionRequest productionRequest) throws ProductionException {
        try {
            return DATE_FORMAT.format(productionRequest.createFromMinMax().getStopDate());
        } catch (ProductionException exception) {
            if (productionRequest.getString("maxDateSource") != null) {
                return productionRequest.getString("maxDateSource");
            } else {
                return DATE_FORMAT.format(MAX_DATE);
            }
        }
    }

    private XMLGregorianCalendar getDateInXmlGregorianCalendarFormat(Date date) {
        GregorianCalendar c = new GregorianCalendar();
        c.setTime(date);
        XMLGregorianCalendar date2 = null;
        try {
            date2 = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
        } catch (DatatypeConfigurationException exception) {
            LOG.log(Level.SEVERE, "cannot instantiate DataType", exception);
        }
        return date2;
    }

    private String getBaseStagingUrl() {
        return "http://"
               + hostName
               + ":" + portNumber
               + "/" + PropertiesWrapper.get("wps.application.name")
               + "/" + PropertiesWrapper.get("staging.directory");
    }

    private List<Map> createProductList() {
        List<Map> productList = new ArrayList<>();
        for (File productionResult : productionResults) {
            Map<String, String> productMap = new HashMap<>();
            productMap.put("productUrl", getBaseStagingUrl() + "/" + productionResult.getName());
            productMap.put("productFileName", productionResult.getName());
            productMap.put("productFileFormat", parseFileFormat(productionResult.getName()));
            productMap.put("productFileSize", Long.toString(productionResult.length()));
            productList.add(productMap);
        }
        return productList;
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

}
