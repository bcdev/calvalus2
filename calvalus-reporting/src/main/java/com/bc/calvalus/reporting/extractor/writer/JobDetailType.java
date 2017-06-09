package com.bc.calvalus.reporting.extractor.writer;

import com.bc.calvalus.reporting.extractor.configuration.Conf;
import com.bc.calvalus.reporting.extractor.counter.CountersType;
import java.time.Instant;
import java.util.List;

/**
 * @author muhammad.bc.
 */
class JobDetailType implements Comparable<String> {

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

    public String getJobId() {
        return jobId;
    }

    public String getUser() {
        return user;
    }

    public String getQueue() {
        return queue;
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

    // use for test only
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    void setJobInfo(com.bc.calvalus.reporting.extractor.jobs.JobType jobType) {
        this.jobId = jobType.getId();
        this.user = jobType.getUser();
        this.queue = jobType.getQueue();
        this.state = jobType.getState();
        this.totalMaps = jobType.getMapsTotal();
        this.startTime = jobType.getStartTime();
        this.finishTime = jobType.getFinishTime();
        this.mapsCompleted = jobType.getMapsCompleted();
        this.reducesCompleted = jobType.getReducesCompleted();
    }

    void setConfInfo(Conf conf) {
        this.inputPath = conf.getPath();
        this.jobName = conf.getJobName();
        this.wpsJobId = conf.getWpsJobId();
        this.remoteRef = conf.getRemoteRef();
        this.remoteUser = conf.getRemoteUser();
        this.processType = conf.getProcessType();
        this.workflowType = conf.getWorkflowType();
        this.inProductType = conf.getInProductType();
        this.dataProcessorUsed = conf.getDataProcessorUsed();
    }

    void setCounterInfo(CountersType countersType) {
        List<com.bc.calvalus.reporting.extractor.counter.CounterType> counter = countersType.getCounterGroup().getCounter();
        for (com.bc.calvalus.reporting.extractor.counter.CounterType counterType : counter) {
            addCounterInfo(counterType);
        }
    }

    private void addCounterInfo(com.bc.calvalus.reporting.extractor.counter.CounterType counterType) {
        String counterTypeName = counterType.getName();

        if (counterTypeName.equalsIgnoreCase("FILE_BYTES_READ")) {
            fileBytesRead = counterType.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("FILE_BYTES_WRITTEN")) {
            fileBytesWritten = counterType.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("HDFS_BYTES_READ")) {
            hdfsBytesRead = counterType.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("HDFS_BYTES_WRITTEN")) {
            hdfsBytesWritten = counterType.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("VCORES_MILLIS_MAPS")) {
            vCoresMillisTotal = counterType.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("MB_MILLIS_MAPS")) {
            mbMillisMapTotal = counterType.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("MB_MILLIS_REDUCES")) {
            mbMillisReduceTotal = counterType.getTotalCounterValue().toString();
        } else if (counterTypeName.equalsIgnoreCase("CPU_MILLISECONDS")) {
            cpuMilliseconds = counterType.getTotalCounterValue().toString();
        }
    }

    @Override
    public int compareTo(String oType) {
        Instant dateTimeInstance = getDateTimeInstance(oType);
        if (this.getDateTimeInstance(this.getFinishTime()).isAfter(dateTimeInstance)) {
            return 1;
        }
        if (this.getDateTimeInstance(this.getFinishTime()).isBefore(dateTimeInstance)) {
            return -1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        JobDetailType jobDetailType = (JobDetailType) obj;
        return jobDetailType.getJobId() == jobId;
    }

    private Instant getDateTimeInstance(String finishTime) {
        long epochMilli = Long.parseLong(finishTime);
        return Instant.ofEpochMilli(epochMilli);
    }
}
