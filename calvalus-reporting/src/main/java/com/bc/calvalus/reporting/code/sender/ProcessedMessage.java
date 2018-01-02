package com.bc.calvalus.reporting.code.sender;

import com.bc.calvalus.reporting.code.reader.JobDetail;

import java.time.LocalDateTime;

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
    private final int inProductsNumber;
    private final long inProductsSize;
    private final String processingCenter;
    private final int configuredCpuCoresPerTask;
    private final long cpuCoreHours;
    private final String processorName;
    private final long configuredRamPerTask;
    private final long ramHours;
    private final String processingWorkflow;
    private final long duration;
    private final String processingStatus;
    private final String outProducts;
    private final int outProductsNumber;
    private final String outCollection;
    private final String outProductsLocation;
    private final long outProductsSize;

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
            int inProductsNumber,
            int inProductsSize,
            String processingCenter,
            int configuredCpuCoresPerTask,
            long cpuCoreHours,
            String processorName,
            long configuredRamPerTask,
            long ramHours,
            String processingWorkflow,
            long duration,
            String processingStatus,
            String outProducts,
            int outProductsNumber,
            String outCollection,
            String outProductsLocation,
            long outProductsSize) {

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

    public ProcessedMessage(JobDetail jobDetail) {
        defaultProductMessage();

        this.requestId = jobDetail.getJobId();
        this.jobName = ""; //TODO: use the right information
        this.jobSubmissionTime = jobDetail.getStartTime();
        this.userName = jobDetail.getUser();
        this.inProducts = ""; //TODO: use the right information
        this.inCollection = jobDetail.getInProductType();
        this.inProductsNumber = 0; //TODO: use the right information
        this.inProductsSize = Integer.parseInt(jobDetail.getFileBytesRead());
        this.processingCenter = "Calvalus";
        this.configuredCpuCoresPerTask = 0; //TODO: use the right information
        this.cpuCoreHours = 0; //TODO: use the right information
        this.processorName = jobDetail.getDataProcessorUsed();
        this.configuredRamPerTask = 0; //TODO: use the right information
        this.ramHours = 0; //TODO: use the right information
        this.processingWorkflow = jobDetail.getWorkflowType();
        this.duration = Long.parseLong(getCalculateDuration(jobDetail.getStartTime(), jobDetail.getFinishTime()));
        this.processingStatus = jobDetail.getState();
        this.outProducts = ""; //TODO: use the right information
        this.outProductsNumber = 0; //TODO: use the right information
        this.outCollection = ""; //TODO: use the right information
        this.outProductsLocation = ""; //TODO: use the right information
        this.outProductsSize = Long.parseLong(jobDetail.getFileBytesWritten());
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

    public int getInProductsNumber() {
        return inProductsNumber;
    }

    public long getInProductsSize() {
        return inProductsSize;
    }

    public String getProcessingCenter() {
        return processingCenter;
    }

    public int getConfiguredCpuCoresPerTask() {
        return configuredCpuCoresPerTask;
    }

    public long getCpuCoreHours() {
        return cpuCoreHours;
    }

    public String getProcessorName() {
        return processorName;
    }

    public long getConfiguredRamPerTask() {
        return configuredRamPerTask;
    }

    public long getRamHours() {
        return ramHours;
    }

    public String getProcessingWorkflow() {
        return processingWorkflow;
    }

    public long getDuration() {
        return duration;
    }

    public String getProcessingStatus() {
        return processingStatus;
    }

    public String getOutProducts() {
        return outProducts;
    }

    public int getOutProductsNumber() {
        return outProductsNumber;
    }

    public String getOutCollection() {
        return outCollection;
    }

    public String getOutProductsLocation() {
        return outProductsLocation;
    }

    public long getOutProductsSize() {
        return outProductsSize;
    }

    private void defaultProductMessage() {
        this.messageType = PRODUCT_PROCESSED_MESSAGE;
        this.serviceId = CODE_DE_PROCESSING_SERVICE;
        this.serviceHost = SERVICE_HOST;
        this.messageTime = LocalDateTime.now().toString();
        this.version = VERSION;
    }

    private String getCalculateDuration(String startTime, String finishTime) {
        long startT = Long.parseLong(startTime);
        long finishT = Long.parseLong(finishTime);
        return Long.toString(finishT - startT);
    }
}
