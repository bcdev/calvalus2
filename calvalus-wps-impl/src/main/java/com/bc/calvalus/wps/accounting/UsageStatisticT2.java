package com.bc.calvalus.wps.accounting;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * @author hans
 */
public class UsageStatisticT2 {

    private String id;
    private Account account;
    private Compound compound;
    private List<Quantity> quantity;
    private String hostName;
    private String timeStamp;
    private String status;

    public UsageStatisticT2(UsageStatisticT2Builder builder) {
        this.id = builder.getJobId();
        this.account = builder.getAccount();
        this.compound = builder.getCompound();
        this.quantity = getQuoteQuantity(builder);
        this.hostName = "www.brockmann-consult.de";
        this.timeStamp = getFormattedTime(builder.getCreationDate());
        this.status = builder.getStatus();
    }

    public String getId() {
        return id;
    }

    public Account getAccount() {
        return account;
    }

    public Compound getCompound() {
        return compound;
    }

    public List<Quantity> getQuantity() {
        return quantity;
    }

    public String getHostName() {
        return hostName;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public String getStatus() {
        return status;
    }

    public String getAsJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }

    private String getFormattedTime(Date date) {
        DateFormat df = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, new Locale("de", "DE"));
        long startTime = date.getTime();
        return df.format(startTime);
    }

    private List<Quantity> getQuoteQuantity(UsageStatisticT2Builder builder) {
        List<Quantity> quantityList = new ArrayList<>();
        quantityList.add(new Quantity("CPU_MILLISECONDS", builder.getCpuMilliSeconds()));
        quantityList.add(new Quantity("PHYSICAL_MEMORY_BYTES", builder.getMemoryBytes()));
        quantityList.add(new Quantity("PROC_VOLUME_BYTES", builder.getVolumeBytes()));
        quantityList.add(new Quantity("PROC_INSTANCE", builder.getInstanceNumber()));
        return quantityList;
    }
}
