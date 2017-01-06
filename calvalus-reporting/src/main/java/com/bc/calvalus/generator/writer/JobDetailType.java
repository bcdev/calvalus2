package com.bc.calvalus.generator.writer;

public class JobDetailType {

    protected String jobId;
    protected String user;
    protected String queue;
    protected String startTime;
    protected String finishTime;
    protected String mapsCompleted;
    protected String reducesCompleted;
    protected String state;
    protected String inputPath;
    protected String fileBytesRead;
    protected String fileBytesWritten;
    protected String hdfsBytesRead;
    protected String hdfsBytesWritten;
    protected String vCoresMillisTotal;
    protected String mbMillisTotal;
    protected String cpuMilliseconds;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    public String getMapsCompleted() {
        return mapsCompleted;
    }

    public void setMapsCompleted(String mapsCompleted) {
        this.mapsCompleted = mapsCompleted;
    }

    public String getReducesCompleted() {
        return reducesCompleted;
    }

    public void setReducesCompleted(String reducesCompleted) {
        this.reducesCompleted = reducesCompleted;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getFileBytesRead() {
        return fileBytesRead;
    }

    public void setFileBytesRead(String fileBytesRead) {
        this.fileBytesRead = fileBytesRead;
    }

    public String getFileBytesWritten() {
        return fileBytesWritten;
    }

    public void setFileBytesWritten(String fileBytesWritten) {
        this.fileBytesWritten = fileBytesWritten;
    }

    public String getHdfsBytesRead() {
        return hdfsBytesRead;
    }

    public void setHdfsBytesRead(String hdfsBytesRead) {
        this.hdfsBytesRead = hdfsBytesRead;
    }

    public String getHdfsBytesWritten() {
        return hdfsBytesWritten;
    }

    public void setHdfsBytesWritten(String hdfsBytesWritten) {
        this.hdfsBytesWritten = hdfsBytesWritten;
    }

    public String getvCoresMillisTotal() {
        return vCoresMillisTotal;
    }

    public void setvCoresMillisTotal(String vCoresMillisTotal) {
        this.vCoresMillisTotal = vCoresMillisTotal;
    }

    public String getMbMillisTotal() {
        return mbMillisTotal;
    }

    public void setMbMillisTotal(String mbMillisTotal) {
        this.mbMillisTotal = mbMillisTotal;
    }

    public String getCpuMilliseconds() {
        return cpuMilliseconds;
    }

    public void setCpuMilliseconds(String cpuMilliseconds) {
        this.cpuMilliseconds = cpuMilliseconds;
    }

    @Override
    public String toString() {
        return "{" +
                "jobId='" + jobId + '\'' +
                ", user='" + user + '\'' +
                ", queue='" + queue + '\'' +
                ", startTime='" + startTime + '\'' +
                ", finishTime='" + finishTime + '\'' +
                ", mapsCompleted='" + mapsCompleted + '\'' +
                ", reducesCompleted='" + reducesCompleted + '\'' +
                ", state='" + state + '\'' +
                ", inputPath='" + inputPath + '\'' +
                ", fileBytesRead='" + fileBytesRead + '\'' +
                ", fileBytesWritten='" + fileBytesWritten + '\'' +
                ", hdfsBytesRead='" + hdfsBytesRead + '\'' +
                ", hdfsBytesWritten='" + hdfsBytesWritten + '\'' +
                ", vCoresMillisTotal='" + vCoresMillisTotal + '\'' +
                ", mbMillisTotal='" + mbMillisTotal + '\'' +
                ", cpuMilliseconds='" + cpuMilliseconds + '\'' +
                '}';
    }
}
