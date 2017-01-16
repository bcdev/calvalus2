package com.bc.calvalus.reporting.ws;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
public class UsageStatisticT2 {

    private static final String BC_PROCESSING_CENTER_JOB_PREFIX = "bc_";

    private String id;
    private String accountPlatform;
    private String accountUserName;
    private String accountRef;
    private String compountId;
    private String compoundName;
    private String compoundType;
    private Map<String, Long> quantity;
    private String hostName;
    private String timeStamp;
    private String status;
    private List<Double> coordinates;

    public UsageStatisticT2(UsageStatistic usageStatistic) {
        this.id = BC_PROCESSING_CENTER_JOB_PREFIX + usageStatistic.getJobId();
        this.accountPlatform = "Brockmann Consult Processing Center";
        this.accountUserName = usageStatistic.getUser(); // may need to be changed to use remote user instead
        this.accountRef = "DUMMY-xx-20170101"; // TODO(hans-permana, 20170116): get the information from the header of incoming WPS request REMOTE_REF, to be stored as a config
        this.compountId = compountId;
        this.compoundName = compoundName;
        this.compoundType = compoundType;
        this.quantity = parseQuantity(usageStatistic);
        this.hostName = hostName;
        this.timeStamp = timeStamp;
        this.status = status;
        this.coordinates = coordinates;
    }

    private Map<String, Long> parseQuantity(UsageStatistic usageStatistic) {
        Map<String, Long> quantity = new HashMap<>();
        quantity.put("CPU_MILLISECONDS", usageStatistic.getCpuMilliseconds());
        quantity.put("PHYSICAL_MEMORY_BYTES", usageStatistic.getMbMillisTotal()); //check with TD if the unit is MB or Bs
        quantity.put("BYTE_READ", usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead());
        quantity.put("BYTE_WRITTEN", usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten());
        quantity.put("PROC_INSTANCE", usageStatistic.getvCoresMillisTotal()); //check with TD if the unit is only the number of instances or for each second (or ms)
        quantity.put("NUM_REQ", 1L);
        return quantity;
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

    public String getCompountId() {
        return compountId;
    }

    public String getCompoundName() {
        return compoundName;
    }

    public String getCompoundType() {
        return compoundType;
    }

    public Map<String, Long> getQuantity() {
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

    public List<Double> getCoordinates() {
        return coordinates;
    }
}
