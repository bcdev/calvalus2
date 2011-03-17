package com.bc.calvalus.production;


import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;

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
    private final WorkflowItem workflow;
    private final boolean autoStaging;
    private final ProductionRequest productionRequest;
    private final String stagingPath;
    private ProcessStatus stagingStatus;

    public Production(String id,
                      String name,
                      String stagingPath,
                      ProductionRequest productionRequest,
                      WorkflowItem workflow) {
        if (id == null) {
            throw new NullPointerException("id");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (productionRequest == null) {
            throw new NullPointerException("productionRequest");
        }
        this.id = id;
        this.name = name;  // todo - check: remove param, instead derive from  productionRequest?
        this.stagingPath = stagingPath;
        this.autoStaging = Boolean.parseBoolean(productionRequest.getProductionParameter("autoStaging"));
        this.workflow = workflow;
        this.productionRequest = productionRequest;
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

    public WorkflowItem getWorkflow() {
        return workflow;
    }

    @Deprecated
    public Object[] getJobIds() {
        return workflow.getJobIds();
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
        return workflow.getStatus();
    }

    public void setProcessingStatus(ProcessStatus processingStatus) {
        this.workflow.setStatus( processingStatus);
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
                ", processingStatus=" + getProcessingStatus() +
                ", stagingStatus=" + stagingStatus +
                '}';
    }

}
