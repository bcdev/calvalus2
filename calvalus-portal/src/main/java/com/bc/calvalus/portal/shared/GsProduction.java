package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.Production} class.
 *
 * @author Norman
 */
public class GsProduction implements IsSerializable {
    String id;
    String name;
    String user;
    String downloadPath;
    GsProcessStatus processingStatus;
    GsProcessStatus stagingStatus;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public GsProduction() {
    }

    public GsProduction(String id,
                        String name,
                        String user,
                        String downloadPath,
                        GsProcessStatus processingStatus,
                        GsProcessStatus stagingStatus) {
        if (id == null) {
            throw new NullPointerException("id");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (user == null) {
            throw new NullPointerException("user");
        }
        if (processingStatus == null) {
            throw new NullPointerException("processingStatus");
        }
        if (stagingStatus == null) {
            throw new NullPointerException("stagingStatus");
        }
        this.id = id;
        this.name = name;
        this.user = user;
        this.downloadPath = downloadPath;
        this.processingStatus = processingStatus;
        this.stagingStatus = stagingStatus;
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

    public String getDownloadPath() {
        return downloadPath;
    }

    public GsProcessStatus getProcessingStatus() {
        return processingStatus;
    }

    public void setProcessingStatus(GsProcessStatus processingStatus) {
        this.processingStatus = processingStatus;
    }

    public GsProcessStatus getStagingStatus() {
        return stagingStatus;
    }

    public void setStagingStatus(GsProcessStatus stagingStatus) {
        this.stagingStatus = stagingStatus;
    }

    @Override
    public String toString() {
        return "Production{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", downloadPath=" + downloadPath +
                ", productionStatus=" + processingStatus +
                ", stagingStatus=" + stagingStatus +
                '}';
    }

}
