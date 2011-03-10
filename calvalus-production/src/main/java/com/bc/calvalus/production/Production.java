package com.bc.calvalus.production;


import com.bc.calvalus.commons.ProcessStatus;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Information about a production.
 *
 * @author Norman
 */
public class Production {
    public static final SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
    private static long uniqueLong = System.nanoTime();
    private final String id;
    private final String name;
    private final String user;
    private final Object[] jobIds;
    private final boolean autoStaging;
    private final ProductionRequest productionRequest;
    private final String stagingPath;
    private ProcessStatus processingStatus;
    private ProcessStatus stagingStatus;

    public Production(String id,
                      String name,
                      String user,
                      String stagingPath,
                      ProductionRequest productionRequest,
                      Object... jobIds) {
        if (id == null) {
            throw new NullPointerException("id");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (user == null) {
            throw new NullPointerException("user");
        }
        if (productionRequest == null) {
            throw new NullPointerException("productionRequest");
        }
        this.id = id;
        this.name = name;  // todo - check: remove param, instead derive from  productionRequest?
        this.user = user; // todo - check: remove param, instead derive from  productionRequest?
        this.stagingPath = stagingPath;
        this.autoStaging = Boolean.parseBoolean(productionRequest.getProductionParameter("autoStaging"));
        this.jobIds = jobIds;
        this.productionRequest = productionRequest;
        this.processingStatus = ProcessStatus.UNKNOWN;
        this.stagingStatus = ProcessStatus.UNKNOWN;
    }

    public static String createId(String productionType) {
        return String.format("%s_%s_%8s",
                             yyyyMMddHHmmss.format(new Date()),
                             productionType,
                             Long.toHexString(nextUniqueLong()));
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getUser() {
        return user;
    }

    public Object[] getJobIds() {
        return jobIds.clone();
    }

    public boolean isAutoStaging() {
        return autoStaging;
    }

    public ProductionRequest getProductionRequest() {
        return productionRequest;
    }

    public String getStagingPath() {
        return stagingPath;
    }

    public ProcessStatus getProcessingStatus() {
        return processingStatus;
    }

    public void setProcessingStatus(ProcessStatus processingStatus) {
        this.processingStatus = processingStatus;
    }

    public ProcessStatus getStagingStatus() {
        return stagingStatus;
    }

    public void setStagingStatus(ProcessStatus stagingStatus) {
        this.stagingStatus = stagingStatus;
    }

    private static long nextUniqueLong() {
        return ++uniqueLong;
    }

    @Override
    public String toString() {
        return "Production{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", stagingPath=" + stagingPath +
                ", productionStatus=" + processingStatus +
                ", stagingStatus=" + stagingStatus +
                '}';
    }

}
