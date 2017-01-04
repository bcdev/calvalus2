package com.bc.calvalus.reporting.ws;

/**
 * @author hans
 */
public class UsageStatistic {

    private String jobId;
    private String user;
    private String queue;
    private long startTime;
    private long finishTime;
    private long totalTime;
    private String state;

    private String inputPath;

    private int mapsCompleted;
    private int reducesCompleted;
    private long fileBytesRead;
    private long fileBytesWritten;
    private long hdfsBytesRead;
    private long hdfsBytesWritten;
    private long mbMillisTotal;
    private long vCoresMillisTotal;
    private long cpuMilliseconds;

    public UsageStatistic(String jobId,
                          String user,
                          String queue,
                          long startTime,
                          long finishTime,
                          String state,
                          String inputPath,
                          int mapsCompleted,
                          int reducesCompleted,
                          long fileBytesRead,
                          long fileBytesWritten,
                          long hdfsBytesRead,
                          long hdfsBytesWritten,
                          long mbMillisTotal,
                          long vCoresMillisTotal,
                          long cpuMilliseconds) {
        this.jobId = jobId;
        this.user = user;
        this.queue = queue;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.totalTime = finishTime - startTime;
        this.state = state;
        this.inputPath = inputPath;
        this.mapsCompleted = mapsCompleted;
        this.reducesCompleted = reducesCompleted;
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

    public String getUser() {
        return user;
    }

    String getQueue() {
        return queue;
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

    String getState() {
        return state;
    }

    public String getInputPath() {
        return inputPath;
    }

    public int getMapsCompleted() {
        return mapsCompleted;
    }

    public int getReducesCompleted() {
        return reducesCompleted;
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
