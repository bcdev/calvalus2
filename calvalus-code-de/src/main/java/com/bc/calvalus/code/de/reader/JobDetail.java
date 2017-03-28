package com.bc.calvalus.code.de.reader;

/**
 * @author muhammad.bc.
 */
public class JobDetail {
    private String user;
    private String queue;
    private String state;
    private String jobId;
    private String jobName;
    private String startTime;
    private String inputPath;
    private String finishTime;
    private String mapsCompleted;
    private String reducesCompleted;
    private String fileBytesRead;
    private String fileBytesWritten;
    private String hdfsBytesRead;
    private String hdfsBytesWritten;
    private String vCoresMillisTotal;
    private String mbMillisMapTotal;
    private String mbMillisReduceTotal;
    private String cpuMilliseconds;
    private String totalMaps;
    private String remoteUser;
    private String remoteRef;
    private String processType;
    private String wpsJobId;
    private String workflowType;
    private String inProductType;
    private String dataProcessorUsed;

    public JobDetail(String user,
                     String queue,
                     String state,
                     String jobId,
                     String jobName,
                     String startTime,
                     String inputPath,
                     String finishTime,
                     String mapsCompleted,
                     String reducesCompleted,
                     String fileBytesRead,
                     String fileBytesWritten,
                     String hdfsBytesRead,
                     String hdfsBytesWritten,
                     String vCoresMillisTotal,
                     String mbMillisMapTotal,
                     String mbMillisReduceTotal,
                     String cpuMilliseconds,
                     String totalMaps,
                     String remoteUser,
                     String remoteRef,
                     String processType,
                     String wpsJobId,
                     String workflowType,
                     String inProductType,
                     String dataProcessorUsed) {
        this.user = user;
        this.queue = queue;
        this.state = state;
        this.jobId = jobId;
        this.jobName = jobName;
        this.startTime = startTime;
        this.inputPath = inputPath;
        this.finishTime = finishTime;
        this.mapsCompleted = mapsCompleted;
        this.reducesCompleted = reducesCompleted;
        this.fileBytesRead = fileBytesRead;
        this.fileBytesWritten = fileBytesWritten;
        this.hdfsBytesRead = hdfsBytesRead;
        this.hdfsBytesWritten = hdfsBytesWritten;
        this.vCoresMillisTotal = vCoresMillisTotal;
        this.mbMillisMapTotal = mbMillisMapTotal;
        this.mbMillisReduceTotal = mbMillisReduceTotal;
        this.cpuMilliseconds = cpuMilliseconds;
        this.totalMaps = totalMaps;
        this.remoteUser = remoteUser;
        this.remoteRef = remoteRef;
        this.processType = processType;
        this.wpsJobId = wpsJobId;
        this.workflowType = workflowType;
        this.inProductType = inProductType;
        this.dataProcessorUsed = dataProcessorUsed;
    }

    public String getUser() {
        return user;
    }

    public String getQueue() {
        return queue;
    }

    public String getState() {
        return state;
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public String getMapsCompleted() {
        return mapsCompleted;
    }

    public String getReducesCompleted() {
        return reducesCompleted;
    }

    public String getFileBytesRead() {
        return fileBytesRead;
    }

    public String getFileBytesWritten() {
        return fileBytesWritten;
    }

    public String getHdfsBytesRead() {
        return hdfsBytesRead;
    }

    public String getHdfsBytesWritten() {
        return hdfsBytesWritten;
    }

    public String getvCoresMillisTotal() {
        return vCoresMillisTotal;
    }

    public String getMbMillisMapTotal() {
        return mbMillisMapTotal;
    }

    public String getMbMillisReduceTotal() {
        return mbMillisReduceTotal;
    }

    public String getCpuMilliseconds() {
        return cpuMilliseconds;
    }

    public String getTotalMaps() {
        return totalMaps;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public String getRemoteRef() {
        return remoteRef;
    }

    public String getProcessType() {
        return processType;
    }

    public String getWpsJobId() {
        return wpsJobId;
    }

    public String getWorkflowType() {
        return workflowType;
    }

    public String getInProductType() {
        return inProductType;
    }

    public String getDataProcessorUsed() {
        return dataProcessorUsed;
    }
}
