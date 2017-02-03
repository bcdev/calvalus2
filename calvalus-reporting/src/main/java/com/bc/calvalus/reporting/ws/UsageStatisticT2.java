package com.bc.calvalus.reporting.ws;

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
    private String accountPlatform;
    private String accountUserName;
    private String accountRef;
    private String compoundId;
    private String compoundName;
    private String compoundType;
    private List<Quantity> quantity;
    private String hostName;
    private String timeStamp;
    private String status;

    public UsageStatisticT2(UsageStatistic usageStatistic) {
        this.id = usageStatistic.getJobId();
        this.accountPlatform = "Brockmann Consult Processing Center";
        this.accountUserName = usageStatistic.getRemoteUser();
        this.accountRef = usageStatistic.getRemoteRef();
        this.compoundId = usageStatistic.getWpsJobId();
        this.compoundName = usageStatistic.getJobName();
        this.compoundType = usageStatistic.getProcessType();
        this.quantity = parseQuantity(usageStatistic);
        this.hostName = "www.brockmann-consult.de";
        this.timeStamp = getFormattedTime(new Date());
        this.status = usageStatistic.getState();
    }

    private String getFormattedTime(Date date) {
        DateFormat df = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, new Locale("de", "DE"));
        long startTime = date.getTime();
        return df.format(startTime);
    }

    private List<Quantity> parseQuantity(UsageStatistic usageStatistic) {
        List<Quantity> quantityList = new ArrayList<>();
        quantityList.add(new Quantity("CPU_MILLISECONDS", usageStatistic.getCpuMilliseconds()));
        quantityList.add(new Quantity("PHYSICAL_MEMORY_BYTES", usageStatistic.getMbMillisTotal())); //check with TD if the unit is MB or Bs
        quantityList.add(new Quantity("BYTE_READ", usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead()));
        quantityList.add(new Quantity("BYTE_WRITTEN", usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten()));
        quantityList.add(new Quantity("PROC_INSTANCE", usageStatistic.getvCoresMillisTotal())); //check with TD if the unit is only the number of instances or for each second (or ms)
        quantityList.add(new Quantity("NUM_REQ", 1L));
        return quantityList;
    }

    public String getId() {
        return id;
    }

    public String getAccountPlatform() {
        return accountPlatform;
    }

    public String getAccountUserName() {
        return accountUserName;
    }

    public String getAccountRef() {
        return accountRef;
    }

    public String getCompoundId() {
        return compoundId;
    }

    public String getCompoundName() {
        return compoundName;
    }

    public String getCompoundType() {
        return compoundType;
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
}
