package com.bc.calvalus.reporting.ws;

/**
 * @author hans
 */
public class UsageStatistic {

    private String jobId;
    private String queueName;
    private long startTime;
    private long finishTime;
    private long totalTime;
    private String status;

    private long fileBytesRead;
    private long fileBytesWritten;
    private long hdfsBytesRead;
    private long hdfsBytesWritten;
    private long mbMillisTotal;
    private long vCoresMillisTotal;
    private long cpuMilliseconds;

    public UsageStatistic(String jobId,
                          String queueName,
                          long startTime,
                          long finishTime,
                          String status,
                          long fileBytesRead,
                          long fileBytesWritten,
                          long hdfsBytesRead,
                          long hdfsBytesWritten,
                          long mbMillisTotal,
                          long vCoresMillisTotal,
                          long cpuMilliseconds) {
        this.jobId = jobId;
        this.queueName = queueName;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.totalTime = finishTime - startTime;
        this.status = status;
        this.fileBytesRead = fileBytesRead;
        this.fileBytesWritten = fileBytesWritten;
        this.hdfsBytesRead = hdfsBytesRead;
        this.hdfsBytesWritten = hdfsBytesWritten;
        this.mbMillisTotal = mbMillisTotal;
        this.vCoresMillisTotal = vCoresMillisTotal;
        this.cpuMilliseconds = cpuMilliseconds;
    }

    public String getJobId() {
        return jobId;
    }

    String getQueueName() {
        return queueName;
    }

    long getStartTime() {
        return startTime;
    }

    long getFinishTime() {
        return finishTime;
    }

    long getTotalTime() {
        return totalTime;
    }

    String getStatus() {
        return status;
    }

    long getFileBytesRead() {
        return fileBytesRead;
    }

    long getFileBytesWritten() {
        return fileBytesWritten;
    }

    long getHdfsBytesRead() {
        return hdfsBytesRead;
    }

    long getHdfsBytesWritten() {
        return hdfsBytesWritten;
    }

    long getMbMillisTotal() {
        return mbMillisTotal;
    }

    long getvCoresMillisTotal() {
        return vCoresMillisTotal;
    }

    long getCpuMilliseconds() {
        return cpuMilliseconds;
    }
}
