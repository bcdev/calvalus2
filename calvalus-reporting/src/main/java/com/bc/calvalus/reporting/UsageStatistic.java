package com.bc.calvalus.reporting;

/**
 * @author hans
 */
class UsageStatistic {

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

    UsageStatistic(String jobId,
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

    String getJobId() {
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

    long getMbMillisMaps() {
        return mbMillisMaps;
    }

    long getMbMillisReduces() {
        return mbMillisReduces;
    }

    long getvCoresMillisMaps() {
        return vCoresMillisMaps;
    }

    long getvCoresMillisReduces() {
        return vCoresMillisReduces;
    }

    long getCpuMilliseconds() {
        return cpuMilliseconds;
    }
}
