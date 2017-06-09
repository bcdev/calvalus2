package com.bc.calvalus.wps.accounting;

import com.bc.calvalus.wps.accounting.Account;
import com.bc.calvalus.wps.accounting.Compound;
import com.bc.calvalus.wps.accounting.UsageStatisticT2;
import java.util.Date;

/**
 * @author hans
 */
public class UsageStatisticT2Builder {

    private String jobId;
    private String remoteRef;
    private Account account;
    private Compound compound;
    private Date creationDate;
    private String status;
    private long cpuMilliSeconds;
    private long memoryBytes;
    private long volumeBytes;
    private long instanceNumber;

    private UsageStatisticT2Builder() {
        this.creationDate = new Date();
    }

    public static UsageStatisticT2Builder create() {
        return new UsageStatisticT2Builder();
    }

    public UsageStatisticT2 build() {
        return new UsageStatisticT2(this);
    }

    public UsageStatisticT2Builder withJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public UsageStatisticT2Builder withAccount(Account account) {
        this.account = account;
        return this;
    }

    public UsageStatisticT2Builder withCompound(Compound compound) {
        this.compound = compound;
        return this;
    }

    public UsageStatisticT2Builder withRemoteRef(String remoteRef) {
        this.remoteRef = remoteRef;
        return this;
    }

    public UsageStatisticT2Builder withStatus(String status) {
        this.status = status;
        return this;
    }

    public UsageStatisticT2Builder withCpuMilliSeconds(long cpuMilliSeconds) {
        this.cpuMilliSeconds = cpuMilliSeconds;
        return this;
    }

    public UsageStatisticT2Builder withMemoryBytes(long memoryBytes) {
        this.memoryBytes = memoryBytes;
        return this;
    }

    public UsageStatisticT2Builder withVolumeBytes(long volumeBytes) {
        this.volumeBytes = volumeBytes;
        return this;
    }

    public UsageStatisticT2Builder withInstanceNumber(long instanceNumber) {
        this.instanceNumber = instanceNumber;
        return this;
    }

    public String getJobId() {
        return jobId;
    }

    public Account getAccount() {
        return account;
    }

    public Compound getCompound() {
        return compound;
    }

    public String getRemoteRef() {
        return remoteRef;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public String getStatus() {
        return status;
    }

    public long getCpuMilliSeconds() {
        return cpuMilliSeconds;
    }

    public long getMemoryBytes() {
        return memoryBytes;
    }

    public long getVolumeBytes() {
        return volumeBytes;
    }

    public long getInstanceNumber() {
        return instanceNumber;
    }
}
