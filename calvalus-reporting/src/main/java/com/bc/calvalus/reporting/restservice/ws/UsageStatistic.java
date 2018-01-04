package com.bc.calvalus.reporting.restservice.ws;

/**
 * @author hans
 */
public class UsageStatistic {

    private String jobId;
    private String user;
    private String queue;
    private long submitTime;
    private long startTime;
    private long finishTime;
    private String state;

    private String outputDir;
    private String remoteUser;
    private String jobName;
    private String remoteRef;
    private String processType;
    private String workflowType;
    private String inputPath;
    private String inProductType;
    private String configuredCpuCores;
    private String configuredRam;

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

    public UsageStatistic(String jobId,
                          String user,
                          String queue,
                          long submitTime,
                          long startTime,
                          long finishTime,
                          String state,
                          String outputDir,
                          String remoteUser,
                          String jobName,
                          String remoteRef,
                          String processType,
                          String workflowType,
                          String inputPath,
                          String inProductType,
                          String configuredCpuCores,
                          String configuredRam,
                          int mapsCompleted,
                          int totalMaps,
                          int reducesCompleted,
                          long fileBytesRead,
                          long fileBytesWritten,
                          long hdfsBytesRead,
                          long hdfsBytesWritten,
                          long mbMillisMapTotal,
                          long mbMillisReduceTotal,
                          long vCoresMillisTotal,
                          long cpuMilliseconds) {
        this.jobId = jobId;
        this.user = user;
        this.queue = queue;
        this.submitTime = submitTime;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.state = state;
        this.outputDir = outputDir;
        this.remoteUser = remoteUser;
        this.jobName = jobName;
        this.remoteRef = remoteRef;
        this.processType = processType;
        this.workflowType = workflowType;
        this.inputPath = inputPath;
        this.inProductType = inProductType;
        this.configuredCpuCores = configuredCpuCores;
        this.configuredRam = configuredRam;
        this.mapsCompleted = mapsCompleted;
        this.totalMaps = totalMaps;
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
        return jobId;
    }

    public String getUser() {
        return user;
    }

    public String getQueue() {
        return queue;
    }

    public long getSubmitTime() {
        return submitTime;
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

    public String getOutputDir() {
        return outputDir;
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

    public String getWorkflowType() {
        return workflowType;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getInProductType() {
        return inProductType;
    }

    public String getConfiguredCpuCores() {
        return configuredCpuCores;
    }

    public String getConfiguredRam() {
        return configuredRam;
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

    public long getMbMillisMapTotal() {
        return mbMillisMapTotal;
    }

    public long getMbMillisReduceTotal() {
        return mbMillisReduceTotal;
    }

    public long getvCoresMillisTotal() {
        return vCoresMillisTotal;
    }

    public long getCpuMilliseconds() {
        return cpuMilliseconds;
    }

    public int getTotalMaps() {
        return totalMaps;
    }
}
