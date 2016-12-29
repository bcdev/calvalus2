package com.bc.calvalus.reporting;

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
    private long mbMillisMaps;
    private long mbMillisReduces;
    private long vCoresMillisMaps;
    private long vCoresMillisReduces;
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
                          long mbMillisMaps,
                          long mbMillisReduces,
                          long vCoresMillisMaps,
                          long vCoresMillisReduces,
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
        this.mbMillisMaps = mbMillisMaps;
        this.mbMillisReduces = mbMillisReduces;
        this.vCoresMillisMaps = vCoresMillisMaps;
        this.vCoresMillisReduces = vCoresMillisReduces;
        this.cpuMilliseconds = cpuMilliseconds;
    }

    public String getJobId() {
        return jobId;
    }

    public String getQueueName() {
        return queueName;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public String getStatus() {
        return status;
    }

    public long getFileBytesRead() {
        return fileBytesRead;
    }

    public long getFileBytesWritten() {
        return fileBytesWritten;
    }

    public long getHdfsBytesRead() {
        return hdfsBytesRead;
    }

    public long getHdfsBytesWritten() {
        return hdfsBytesWritten;
    }

    public long getMbMillisMaps() {
        return mbMillisMaps;
    }

    public long getMbMillisReduces() {
        return mbMillisReduces;
    }

    public long getvCoresMillisMaps() {
        return vCoresMillisMaps;
    }

    public long getvCoresMillisReduces() {
        return vCoresMillisReduces;
    }

    public long getCpuMilliseconds() {
        return cpuMilliseconds;
    }
}
