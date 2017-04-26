package com.bc.calvalus.wps.accounting;

import com.bc.calvalus.wps.accounting.Account;
import com.bc.calvalus.wps.accounting.AccountBuilder;
import com.bc.calvalus.wps.accounting.Compound;
import com.bc.calvalus.wps.accounting.CompoundBuilder;
import com.bc.calvalus.wps.accounting.Quantity;
import com.bc.calvalus.wps.accounting.UsageStatistic;
import com.bc.calvalus.wps.accounting.UsageStatisticT2Builder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class UsageStatisticT2 {
    private String id;
    private Account account;
    private Compound compound;
    private List<Quantity> quantity;
    private String hostName;
    private String timeStamp;
    private String status;

    public UsageStatisticT2(UsageStatistic usageStatistic) {
        this.id = usageStatistic.getJobId();
        this.account = AccountBuilder.create().withPlatform("Brockmann Consult Processing Center").withUsername(usageStatistic.getUser()).withRef(usageStatistic.getRemoteRef()).build();
        this.compound = CompoundBuilder.create().withId(this.extractCompoundId(usageStatistic.getOutputDir())).withName(usageStatistic.getJobName()).withType(usageStatistic.getProcessType()).build();
        this.quantity = this.parseQuantity(usageStatistic);
        this.hostName = "www.brockmann-consult.de";
        this.timeStamp = this.getFormattedTime(new Date());
        this.status = usageStatistic.getState();
    }

    public UsageStatisticT2(UsageStatisticT2Builder builder) {
        this.id = builder.getJobId();
        this.account = builder.getAccount();
        this.compound = builder.getCompound();
        this.quantity = this.getQuoteQuantity(builder);
        this.hostName = "www.brockmann-consult.de";
        this.timeStamp = this.getFormattedTime(builder.getCreationDate());
        this.status = builder.getStatus();
    }

    public String getId() {
        return this.id;
    }

    public Account getAccount() {
        return this.account;
    }

    public Compound getCompound() {
        return this.compound;
    }

    public List<Quantity> getQuantity() {
        return this.quantity;
    }

    public String getHostName() {
        return this.hostName;
    }

    public String getTimeStamp() {
        return this.timeStamp;
    }

    public String getStatus() {
        return this.status;
    }

    public String getAsJson() {
        Gson gson = (new GsonBuilder()).setPrettyPrinting().create();
        return gson.toJson(this);
    }

    private String extractCompoundId(String outputDir) {
        return outputDir.substring(outputDir.lastIndexOf("/") + 1);
    }

    private String getFormattedTime(Date date) {
        DateFormat df = DateFormat.getDateTimeInstance(2, 2, new Locale("de", "DE"));
        long startTime = date.getTime();
        return df.format(Long.valueOf(startTime));
    }

    private List<Quantity> parseQuantity(UsageStatistic usageStatistic) {
        ArrayList quantityList = new ArrayList();
        quantityList.add(new Quantity("CPU_MILLISECONDS", usageStatistic.getCpuMilliseconds()));
        quantityList.add(new Quantity("PHYSICAL_MEMORY_BYTES", usageStatistic.getMbMillisMapTotal() + usageStatistic.getMbMillisReduceTotal()));
        quantityList.add(new Quantity("BYTE_READ", usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead()));
        quantityList.add(new Quantity("BYTE_WRITTEN", usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten()));
        quantityList.add(new Quantity("PROC_INSTANCE", usageStatistic.getvCoresMillisTotal()));
        quantityList.add(new Quantity("NUM_REQ", 1L));
        return quantityList;
    }

    private List<Quantity> getQuoteQuantity(UsageStatisticT2Builder builder) {
        ArrayList quantityList = new ArrayList();
        quantityList.add(new Quantity("CPU_MILLISECONDS", builder.getCpuMilliSeconds()));
        quantityList.add(new Quantity("PHYSICAL_MEMORY_BYTES", builder.getMemoryBytes()));
        quantityList.add(new Quantity("PROC_VOLUME_BYTES", builder.getVolumeBytes()));
        quantityList.add(new Quantity("PROC_INSTANCE", builder.getInstanceNumber()));
        return quantityList;
    }
}
