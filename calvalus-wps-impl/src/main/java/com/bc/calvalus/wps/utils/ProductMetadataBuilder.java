package com.bc.calvalus.wps.utils;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs.MAX_DATE;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs.MIN_DATE;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
import com.sun.jersey.api.uri.UriBuilderImpl;

import javax.ws.rs.core.UriBuilder;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.File;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
    private String processorId;
    private String productionType;
    private String outputFormat;
    private List<Map> productList;
    private boolean isLocal;

    private Production production;
    private List<File> productionResults;
    private WpsServerContext serverContext;
    private Map<String, Object> processParameters;
    private WpsProcess processor;
    private String hostName;
    private int portNumber;
    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final ZonedDateTime DEFAULT_MIN_TIME = ZonedDateTime.ofInstant(new Date(MIN_DATE).toInstant(), ZoneId.systemDefault());
    private static final ZonedDateTime DEFAULT_MAX_TIME = ZonedDateTime.ofInstant(new Date(MAX_DATE).toInstant(), ZoneId.systemDefault());


    private ProductMetadataBuilder() {
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

    public ProductMetadataBuilder withProcessor(WpsProcess processor) {
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

    public String getProductionName() {
        return productionName;
    }

    public String getProductionType() {
        return productionType;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    String getJobFinishTime() {
        return jobFinishTime;
    }

    String getProductOutputDir() {
        return productOutputDir;
    }

    String getProcessName() {
        return processName;
    }

    String getProcessorId() {
        return processorId;
    }

    String getInputDatasetName() {
        return inputDatasetName;
    }

    String getRegionWkt() {
        return regionWkt;
    }


    String getRegionBox() {
        return regionBox;
    }

    String getStartDate() {
        return startDate;
    }

    String getStopDate() {
        return stopDate;
    }

    String getCollectionUrl() {
        return collectionUrl;
    }

    String getProcessorVersion() {
        return processorVersion;
    }

    List<Map> getProductList() {
        return productList;
    }

    public Production getProduction() {
        return production;
    }

    public WpsServerContext getServerContext() {
        return serverContext;
    }

    public ProductMetadata build() throws ProductMetadataException {
        if (!isLocal) {
            ProductionRequest productionRequest = this.production.getProductionRequest();
            this.jobFinishTime = getDateInXmlGregorianCalendarFormat(this.production.getWorkflow().getStopTime()).toString();
            this.productOutputDir = productionRequest.getStagingDirectory(this.production.getId());
            this.productionName = this.production.getName();
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
                this.processorId = getProcessorId(productionRequest);
                this.productionType = parseProductionType(productionRequest.getString("productionType"));
                this.outputFormat = productionRequest.getString("outputFormat");
            } catch (ProductionException exception) {
                throw new ProductMetadataException("Unable to create product metadata", exception);
            }
            this.productList = createProductList();
        } else {
            this.jobFinishTime = getDateInXmlGregorianCalendarFormat(new Date()).toString();
            this.productionName = (String) processParameters.get("productionName");
            this.collectionUrl = getBaseStagingUrl() + "/" + productOutputDir + "/";
            this.processName = processor.getIdentifier().split("~")[2];
            this.inputDatasetName = (String) processParameters.get("sourceProduct");
            String regionWktRaw = (String) processParameters.get("geoRegion");
            this.regionWkt = extractRegionWkt(regionWktRaw);
            this.regionBox = parseRegionBox();
            this.startDate = DEFAULT_MIN_TIME.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            this.stopDate = DEFAULT_MAX_TIME.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            this.processorVersion = processor.getVersion();
            this.processorId = processor.getIdentifier();
            this.productionType = parseProductionType((String) processParameters.get("productionType"));
            this.outputFormat = (String) processParameters.get("outputFormat");
            this.productList = createProductList();
        }
        return new ProductMetadata(this);
    }

    private String getProcessorId(ProductionRequest productionRequest) throws ProductionException {
        String processorBundle = productionRequest.getString("processorBundleName");
        String processorBundleVersion = productionRequest.getString("processorBundleVersion");
        String processorName = productionRequest.getString("processorName");
        return processorBundle + "~" + processorBundleVersion + "~" + processorName;
    }

    private String parseProductionType(String productionType) {
        if (productionType.toLowerCase().contains("l1a")) {
            return "1A";
        } else if (productionType.toLowerCase().contains("l1b")) {
            return "1B";
        } else if (productionType.toLowerCase().contains("l2")) {
            return "2";
        } else if (productionType.toLowerCase().contains("l3")) {
            return "3";
        } else {
            return "UNKNOWN";
        }
    }

    private String extractRegionWkt(String regionWkt) {
        String[] elements = regionWkt.split("[(, )]+");
        if (!"polygon".equals(elements[0].toLowerCase())) {
            throw new IllegalArgumentException(regionWkt);
        }
        StringBuilder accu = new StringBuilder();
        for (int i = 1; i < elements.length; i += 2) {
            if (accu.length() > 0) {
                accu.append(" ");
            }
            accu.append(elements[i + 1]);
            accu.append(" ");
            accu.append(elements[i]);
        }
        return accu.toString();
    }

    private String parseRegionBox() {
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
            return getDateInXmlGregorianCalendarFormat(productionRequest.createFromMinMax().getStartDate()).toString();
        } catch (ProductionException exception) {
            if (productionRequest.getString("minDateSource") != null) {
                return productionRequest.getString("minDateSource");
            } else {
                return DEFAULT_MIN_TIME.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            }
        }
    }

    private String getStopDate(ProductionRequest productionRequest) throws ProductionException {
        try {
            return getDateInXmlGregorianCalendarFormat(productionRequest.createFromMinMax().getStopDate()).toString();
        } catch (ProductionException exception) {
            if (productionRequest.getString("maxDateSource") != null) {
                return productionRequest.getString("maxDateSource");
            } else {
                return DEFAULT_MAX_TIME.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
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
        UriBuilder builder = new UriBuilderImpl();
        return builder.scheme("http")
                    .host(hostName)
                    .port(portNumber)
                    .path(PropertiesWrapper.get("wps.application.name"))
                    .path(PropertiesWrapper.get("staging.directory"))
                    .build().toString();
    }

    private List<Map> createProductList() {
        List<Map> productList = new ArrayList<>();
        for (File productionResult : productionResults) {
            Map<String, String> productMap = new HashMap<>();
            productMap.put("productUrl", getBaseStagingUrl() + "/" + productOutputDir + "/" + productionResult.getName());
            productMap.put("productFileName", productionResult.getName());
            productMap.put("productFileFormat", parseFileFormat(productionResult.getName()));
            productMap.put("productMimeType", parseMimeType(productionResult.getName()));
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

    private String parseMimeType(String fileName) {
        if (fileName.toLowerCase().endsWith(".zip")) {
            return "application/zip";
        } else if (fileName.toLowerCase().endsWith(".xml")) {
            return "application/xml";
        } else if (fileName.toLowerCase().endsWith("-metadata")) {
            return "application/xml";
        } else if (fileName.toLowerCase().endsWith("nc")) {
            return "application/netcdf";
        } else if (fileName.toLowerCase().endsWith("tif")) {
            return "image/tiff";
        } else {
            return "application/octet-stream";
        }
    }

}
