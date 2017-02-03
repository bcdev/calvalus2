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
    private String state;

    private String wpsJobId;
    private String remoteUser;
    private String jobName;
    private String remoteRef;
    private String processType;
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
                          String wpsJobId,
                          String remoteUser,
                          String jobName,
                          String remoteRef,
                          String processType,
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
        this.state = state;
        this.wpsJobId = wpsJobId;
        this.remoteUser = remoteUser;
        this.jobName = jobName;
        this.remoteRef = remoteRef;
        this.processType = processType;
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

    public String getQueue() {
        return queue;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    long getTotalTime() {
        return finishTime - startTime;
    }

    public String getState() {
        return state;
    }

    public String getWpsJobId() {
        return wpsJobId;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public String getJobName() {
        return jobName;
    }

    public String getRemoteRef() {
        return remoteRef;
    }

    public String getProcessType() {
        return processType;
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

    public long getMbMillisTotal() {
        return mbMillisTotal;
    }

    public long getvCoresMillisTotal() {
        return vCoresMillisTotal;
    }

    public long getCpuMilliseconds() {
        return cpuMilliseconds;
    }
}
