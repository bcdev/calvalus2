package com.bc.calvalus.reporting.code.sender;

import com.bc.calvalus.reporting.urban.reporting.CalvalusReport;
import com.google.gson.Gson;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Locale;

/**
 * @author muhammad.bc.
 */
public class ProcessedMessage {

    private static final String PRODUCT_PROCESSED_MESSAGE = "ProductProcessedMessage";
    private static final String CODE_DE_PROCESSING_SERVICE = "code-de-processing-service";
    private static final String VERSION = "1.0";
    private static final String SERVICE_HOST = "cd-processing";

    private final String requestId;
    private final String jobName;
    private final String jobSubmissionTime;
    private final String userName;
    private final String inProducts;
    private final String inCollection;
    private final long inProductsNumber;
    private final double inProductsSize;
    private final String processingCenter;
    private final long configuredCpuCoresPerTask;
    private final double cpuCoreHours;
    private final String processorName;
    private final double configuredRamPerTask;
    private final double ramHours;
    private final String processingWorkflow;
    private final double duration;
    private final String processingStatus;
    private final String outProducts;
    private final long outProductsNumber;
    private final String outCollection;
    private final String outProductsLocation;
    private final double outProductsSize;

    private String messageType;
    private String serviceId;
    private String serviceHost;
    private String messageTime;
    private String version;

    public ProcessedMessage(
                String requestId,
                String jobName,
                String jobSubmissionTime,
                String userName,
                String inProducts,
                String inCollection,
                long inProductsNumber,
                double inProductsSize,
                String processingCenter,
                long configuredCpuCoresPerTask,
                double cpuCoreHours,
                String processorName,
                double configuredRamPerTask,
                double ramHours,
                String processingWorkflow,
                long duration,
                String processingStatus,
                String outProducts,
                long outProductsNumber,
                String outCollection,
                String outProductsLocation,
                double outProductsSize) {

        defaultProductMessage();

        this.requestId = requestId;
        this.jobName = jobName;
        this.jobSubmissionTime = jobSubmissionTime;
        this.userName = userName;
        this.inProducts = inProducts;
        this.inCollection = inCollection;
        this.inProductsNumber = inProductsNumber;
        this.inProductsSize = inProductsSize;
        this.processingCenter = processingCenter;
        this.configuredCpuCoresPerTask = configuredCpuCoresPerTask;
        this.cpuCoreHours = cpuCoreHours;
        this.processorName = processorName;
        this.configuredRamPerTask = configuredRamPerTask;
        this.ramHours = ramHours;
        this.processingWorkflow = processingWorkflow;
        this.duration = duration;
        this.processingStatus = processingStatus;
        this.outProducts = outProducts;
        this.outProductsNumber = outProductsNumber;
        this.outCollection = outCollection;
        this.outProductsLocation = outProductsLocation;
        this.outProductsSize = outProductsSize;
    }

    public ProcessedMessage(CalvalusReport calvalusReport) {
        defaultProductMessage();

        this.requestId = calvalusReport.getJobId();
        this.jobName = calvalusReport.getJobName();
        this.jobSubmissionTime = convertMillisToIsoString(calvalusReport.getStartTime());
        this.userName = calvalusReport.getUser();
        this.inProducts = calvalusReport.getInputPath();
        this.inCollection = ""; //TODO: get the right information in the usage statistics
        this.inProductsNumber = calvalusReport.getMapsCompleted();
        this.inProductsSize = getGbFromBytes(calvalusReport.getFileBytesRead());
        this.processingCenter = "Calvalus";
        this.configuredCpuCoresPerTask = 0; //TODO: get the right information in the usage statistics
        this.cpuCoreHours = calvalusReport.getCpuMilliseconds() / (3600.0 * 1000.0);
        this.processorName = ""; //TODO: get the right information in the usage statistics
        this.configuredRamPerTask = 0; //TODO: get the right information in the usage statistics
        this.ramHours = calculateRamHours(calvalusReport.getMbMillisMapTotal(),
                                          calvalusReport.getMbMillisReduceTotal());
        this.processingWorkflow = ""; //TODO: get the right information in the usage statistics
        this.duration = (calvalusReport.getFinishTime() - calvalusReport.getStartTime()) / 1000.0;
        this.processingStatus = calvalusReport.getState();
        this.outProducts = ""; //TODO: get the right information in the usage statistics
        this.outProductsNumber = calvalusReport.getReducesCompleted();
        this.outCollection = ""; //TODO: get the right information in the usage statistics
        this.outProductsLocation = ""; //TODO: get the right information in the usage statistics
        this.outProductsSize = getGbFromBytes(calvalusReport.getFileBytesWritten());
    }

    private double getGbFromBytes(long fileBytesRead) {
        return fileBytesRead / (1024.0 * 1024.0 * 1024.0);
    }

    private String convertMillisToIsoString(long timeMillis) {
        Date date = new Date(timeMillis);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);
        return dateFormat.format(date);
    }

    private double calculateRamHours(long mbMillisMapTotal, long mbMillisReduceTotal) {
        return (mbMillisMapTotal + mbMillisReduceTotal) / (1024.0 * 3600.0 * 1000.0);
    }

    public String getRequestId() {
        return requestId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobSubmissionTime() {
        return jobSubmissionTime;
    }

    public String getUserName() {
        return userName;
    }

    public String getInProducts() {
        return inProducts;
    }

    public String getInCollection() {
        return inCollection;
    }

    public long getInProductsNumber() {
        return inProductsNumber;
    }

    public double getInProductsSize() {
        return inProductsSize;
    }

    public String getProcessingCenter() {
        return processingCenter;
    }

    public long getConfiguredCpuCoresPerTask() {
        return configuredCpuCoresPerTask;
    }

    public double getCpuCoreHours() {
        return cpuCoreHours;
    }

    public String getProcessorName() {
        return processorName;
    }

    public double getConfiguredRamPerTask() {
        return configuredRamPerTask;
    }

    public double getRamHours() {
        return ramHours;
    }

    public String getProcessingWorkflow() {
        return processingWorkflow;
    }

    public double getDuration() {
        return duration;
    }

    public String getProcessingStatus() {
        return processingStatus;
    }

    public String getOutProducts() {
        return outProducts;
    }

    public long getOutProductsNumber() {
        return outProductsNumber;
    }

    public String getOutCollection() {
        return outCollection;
    }

    public String getOutProductsLocation() {
        return outProductsLocation;
    }

    public double getOutProductsSize() {
        return outProductsSize;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    private void defaultProductMessage() {
        this.messageType = PRODUCT_PROCESSED_MESSAGE;
        this.serviceId = CODE_DE_PROCESSING_SERVICE;
        this.serviceHost = SERVICE_HOST;
        this.messageTime = LocalDateTime.now().toString();
        this.version = VERSION;
    }
}
