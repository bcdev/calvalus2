package com.bc.calvalus.code.de.sender;

import com.bc.calvalus.code.de.reader.JobDetail;

/**
 * @author muhammad.bc.
 */
public class ProcessedMessage {
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
    private String outProductSize;



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
