package com.bc.calvalus.code.de.sender;

import com.bc.calvalus.code.de.reader.JobDetail;
import java.time.LocalDateTime;

/**
 * @author muhammad.bc.
 */
public class ProcessedMessage {
    public static final String PRODUCT_PROCESSED_MESSAGE = "ProductProcessedMessage";
    public static final String CODE_DE_PROCESSING_SERVICE = "code-de-processing-service";
    public static final String VERSION = "1.0";
    public static final String SERVICE_HOST = "cd-proxy.eoc.dlr.de";
    private String requestId;
    private String jobSubmissionTime;
    private String userName;
    private String inProductType;
    private String inProductSize;
    private String host;
    private String coreHours;
    private String processorName;
    private String processMemory;
    private String processingWorkflow;
    private String duration;
    private String processingStatus;
    private final String outProductSize;
    private String messageType;
    private String serviceId;
    private String serviceHost;
    private String messageTime;
    private String version;


    public ProcessedMessage(
            String requestId,
            String jobSubmissionTime,
            String userName,
            String inProductType,
            String inProductSize,
            String host,
            String coreHours,
            String processorName,
            String processMemory,
            String processingWorkflow,
            String duration,
            String processingStatus,
            String outProductSize) {

        defaultProductMessage();

        this.requestId = requestId;
        this.jobSubmissionTime = jobSubmissionTime;
        this.userName = userName;
        this.inProductType = inProductType;
        this.inProductSize = inProductSize;
        this.host = host;
        this.coreHours = coreHours;
        this.processorName = processorName;
        this.processMemory = processMemory;
        this.processingWorkflow = processingWorkflow;
        this.duration = duration;
        this.processingStatus = processingStatus;
        this.outProductSize = outProductSize;
    }

    public ProcessedMessage(JobDetail jobDetail) {

        defaultProductMessage();


        this.requestId = jobDetail.getJobId();
        this.jobSubmissionTime = jobDetail.getStartTime();
        this.userName = jobDetail.getUser();
        this.inProductType = jobDetail.getInProductType();
        this.inProductSize = jobDetail.getFileBytesRead();
        this.host = "Calvalus";
        this.coreHours = "Core";
        this.processorName = jobDetail.getDataProcessorUsed();
        this.processMemory = jobDetail.getCpuMilliseconds();
        this.processingWorkflow = jobDetail.getWorkflowType();
        this.duration = getCalculateDuration(jobDetail.getStartTime(), jobDetail.getFinishTime());
        this.processingStatus = jobDetail.getState();
        this.outProductSize = jobDetail.getFileBytesWritten();
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

    public String getMessageType() {
        return messageType;
    }

    public String getServiceId() {
        return serviceId;
    }

    public String getServiceHost() {
        return serviceHost;
    }

    public String getMessageTime() {
        return messageTime;
    }

    public String getVersion() {
        return version;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getJobSubmissionTime() {
        return jobSubmissionTime;
    }

    public String getUserName() {
        return userName;
    }

    public String getInProductType() {
        return inProductType;
    }

    public String getInProductSize() {
        return inProductSize;
    }

    public String getHost() {
        return host;
    }

    public String getCoreHours() {
        return coreHours;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getProcessMemory() {
        return processMemory;
    }

    public String getProcessingWorkflow() {
        return processingWorkflow;
    }

    public String getDuration() {
        return duration;
    }

    public String getProcessingStatus() {
        return processingStatus;
    }

    public String getOutProductSize() {
        return outProductSize;
    }
}
