package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.Production} class.
 *
 * @author Norman
 */
public class DtoProduction implements IsSerializable {
    String id;
    String name;
    String user;
    String archivePath;
    String downloadPath;
    boolean autoStaging;
    DtoProcessStatus processingStatus;
    DtoProcessStatus stagingStatus;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProduction() {
    }

    public DtoProduction(String id,
                         String name,
                         String user,
                         String archivePath,
                         String downloadPath,
                         boolean autoStaging,
                         DtoProcessStatus processingStatus,
                         DtoProcessStatus stagingStatus) {
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
        this.archivePath = archivePath;
        this.downloadPath = downloadPath;
        this.autoStaging = autoStaging;
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

    public String getInventoryPath() {
        return archivePath;
    }

    public String getDownloadPath() {
        return downloadPath;
    }

    public boolean isAutoStaging() {
        return autoStaging;
    }

    public DtoProcessStatus getProcessingStatus() {
        return processingStatus;
    }

    public void setProcessingStatus(DtoProcessStatus processingStatus) {
        this.processingStatus = processingStatus;
    }

    public DtoProcessStatus getStagingStatus() {
        return stagingStatus;
    }

    public void setStagingStatus(DtoProcessStatus stagingStatus) {
        this.stagingStatus = stagingStatus;
    }

    @Override
    public String toString() {
        return "Production{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", downloadPath=" + downloadPath +
                ", processingStatus=" + processingStatus +
                ", stagingStatus=" + stagingStatus +
                '}';
    }

}
