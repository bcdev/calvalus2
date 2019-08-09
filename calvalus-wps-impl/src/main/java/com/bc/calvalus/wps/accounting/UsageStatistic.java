package com.bc.calvalus.wps.accounting;

public class UsageStatistic {
    private String jobId;
    private String user;
    private String queue;
    private long startTime;
    private long finishTime;
    private String state;
    private String outputDir;
    private String remoteUser;
    private String jobName;
    private String remoteRef;
    private String processType;
    private String inputPath;
    private int mapsCompleted;
    private int totalMaps;
    private int reducesCompleted;
    private long fileBytesRead;
    private long fileBytesWritten;
    private long hdfsBytesRead;
    private long hdfsBytesWritten;
    private long mbMillisMapTotal;
    private long mbMillisReduceTotal;
    private long vCoresMillisTotal;
    private long cpuMilliseconds;

    public UsageStatistic(String jobId, String user, String queue, long startTime, long finishTime, String state, String outputDir, String remoteUser, String jobName, String remoteRef, String processType, String inputPath, int mapsCompleted, int reducesCompleted, long fileBytesRead, long fileBytesWritten, long hdfsBytesRead, long hdfsBytesWritten, long mbMillisMapTotal, long mbMillisReduceTotal, long vCoresMillisTotal, long cpuMilliseconds) {
        this.jobId = jobId;
        this.user = user;
        this.queue = queue;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.state = state;
        this.outputDir = outputDir;
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
        this.mbMillisReduceTotal = mbMillisReduceTotal;
        this.mbMillisMapTotal = mbMillisMapTotal;
        this.vCoresMillisTotal = vCoresMillisTotal;
        this.cpuMilliseconds = cpuMilliseconds;
    }

    public String getJobId() {
        return this.jobId;
    }

    public String getUser() {
        return this.user;
    }

    public String getQueue() {
        return this.queue;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public long getFinishTime() {
        return this.finishTime;
    }

    long getTotalTime() {
        return this.finishTime - this.startTime;
    }

    public String getState() {
        return this.state;
    }

    public String getOutputDir() {
        return this.outputDir;
    }

    public String getRemoteUser() {
        return this.remoteUser;
    }

    public String getJobName() {
        return this.jobName;
    }

    public String getRemoteRef() {
        return this.remoteRef;
    }

    public String getProcessType() {
        return this.processType;
    }

    public String getInputPath() {
        return this.inputPath;
    }

    public int getMapsCompleted() {
        return this.mapsCompleted;
    }

    public int getReducesCompleted() {
        return this.reducesCompleted;
    }

    public long getFileBytesRead() {
        return this.fileBytesRead;
    }

    public long getFileBytesWritten() {
        return this.fileBytesWritten;
    }

    public long getHdfsBytesRead() {
        return this.hdfsBytesRead;
    }

    public long getHdfsBytesWritten() {
        return this.hdfsBytesWritten;
    }

    public long getMbMillisMapTotal() {
        return this.mbMillisMapTotal;
    }

    public long getMbMillisReduceTotal() {
        return this.mbMillisReduceTotal;
    }

    public long getvCoresMillisTotal() {
        return this.vCoresMillisTotal;
    }

    public long getCpuMilliseconds() {
        return this.cpuMilliseconds;
    }

    public int getTotalMaps() {
        return this.totalMaps;
    }
}
