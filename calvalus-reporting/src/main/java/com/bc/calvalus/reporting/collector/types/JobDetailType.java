package com.bc.calvalus.reporting.collector.types;


import java.time.Instant;
import java.util.List;

/**
 * @author muhammad.bc.
 */
public class JobDetailType {

    private String user;
    private String queue;
    private String state;
    private String jobId;
    private String jobName;
    private String submitTime;
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
    private String outputDir;
    private String configuredCpuCores;
    private String configuredRam;

    public String getJobId() {
        return jobId;
    }

    public String getUser() {
        return user;
    }

    public String getQueue() {
        return queue;
    }

    public String getSubmitTime() {
        return submitTime;
    }

    public String getStartTime() {
        return startTime;
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

    public String getState() {
        return state;
    }

    public String getInputPath() {
        return inputPath;
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

    public String getJobName() {
        return jobName;
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

    public String getOutputDir() {
        return outputDir;
    }

    public String getConfiguredCpuCores() {
        return configuredCpuCores;
    }

    public String getConfiguredRam() {
        return configuredRam;
    }

    // use for test only
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    public void setJobInfo(Job job) {
        this.jobId = job.getId();
        this.user = job.getUser();
        this.queue = job.getQueue();
        this.state = job.getState();
        this.totalMaps = job.getMapsTotal();
        this.submitTime = job.getSubmitTime();
        this.startTime = job.getStartTime();
        this.finishTime = job.getFinishTime();
        this.mapsCompleted = job.getMapsCompleted();
        this.reducesCompleted = job.getReducesCompleted();
    }

    public void setConfInfo(JobConf conf) {
        this.inputPath = conf.getPath();
        this.jobName = conf.getJobName();
        this.wpsJobId = conf.getWpsJobId();
        this.remoteRef = conf.getRemoteRef();
        this.remoteUser = conf.getRemoteUser();
        this.processType = conf.getProcessType();
        this.workflowType = conf.getWorkflowType();
        this.inProductType = conf.getInProductType();
        this.dataProcessorUsed = conf.getDataProcessorUsed();
        this.outputDir = conf.getOutputDir();
        this.configuredCpuCores = conf.getConfiguredCpuCores();
        this.configuredRam = conf.getConfiguredRam();
    }

    public void setCounterInfo(JobCounters jobCounters) {
        List<JobCounter> counter = jobCounters.getCounterGroup().getCounter();
        for (JobCounter jobCounter : counter) {
            addCounterInfo(jobCounter);
        }
    }

    private void addCounterInfo(JobCounter jobCounter) {
        String counterTypeName = jobCounter.getName();

        if (counterTypeName.equalsIgnoreCase("FILE_BYTES_READ")) {
            fileBytesRead = jobCounter.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("FILE_BYTES_WRITTEN")) {
            fileBytesWritten = jobCounter.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("HDFS_BYTES_READ")) {
            hdfsBytesRead = jobCounter.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("HDFS_BYTES_WRITTEN")) {
            hdfsBytesWritten = jobCounter.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("VCORES_MILLIS_MAPS")) {
            vCoresMillisTotal = jobCounter.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("MB_MILLIS_MAPS")) {
            mbMillisMapTotal = jobCounter.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("MB_MILLIS_REDUCES")) {
            mbMillisReduceTotal = jobCounter.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("CPU_MILLISECONDS")) {
            cpuMilliseconds = jobCounter.getTotalCounterValue().toString();
        }
    }

    private Instant getDateTimeInstance(String finishTime) {
        long epochMilli = Long.parseLong(finishTime);
        return Instant.ofEpochMilli(epochMilli);
    }
}
