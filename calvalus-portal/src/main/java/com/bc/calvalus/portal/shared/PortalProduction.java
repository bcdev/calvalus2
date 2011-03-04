package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * Information about a production.
 *
 * @author Norman
 */
public class PortalProduction implements IsSerializable {
    String id;
    String name;
    String outputUrl;
    PortalProductionStatus processingStatus;
    PortalProductionStatus stagingStatus;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public PortalProduction() {
    }

    public PortalProduction(String id,
                            String name,
                            String outputUrl,
                            PortalProductionStatus processingStatus,
                            PortalProductionStatus stagingStatus) {
        if (id == null) {
            throw new NullPointerException("id");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (processingStatus == null) {
            throw new NullPointerException("processingStatus");
        }
        if (stagingStatus == null) {
            throw new NullPointerException("stagingStatus");
        }
        this.id = id;
        this.name = name;
        this.outputUrl = outputUrl;
        this.processingStatus = processingStatus;
        this.stagingStatus = stagingStatus;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getOutputUrl() {
        return outputUrl;
    }

    public PortalProductionStatus getProcessingStatus() {
        return processingStatus;
    }

    public void setProcessingStatus(PortalProductionStatus processingStatus) {
        this.processingStatus = processingStatus;
    }

    public PortalProductionStatus getStagingStatus() {
        return stagingStatus;
    }

    public void setStagingStatus(PortalProductionStatus stagingStatus) {
        this.stagingStatus = stagingStatus;
    }

    @Override
    public String toString() {
        return "Production{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", outputUrl=" + outputUrl +
                ", productionStatus=" + processingStatus +
                ", stagingStatus=" + stagingStatus +
                '}';
    }

}
