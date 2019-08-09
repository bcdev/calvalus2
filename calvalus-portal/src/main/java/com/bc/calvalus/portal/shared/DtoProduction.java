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
    String productionType;
    String archivePath;
    String downloadPath;
    boolean autoStaging;
    DtoProcessStatus processingStatus;
    DtoProcessStatus stagingStatus;
    private String[] additionalStagingPaths;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProduction() {
    }

    public DtoProduction(String id,
                         String name,
                         String user,
                         String productionType,
                         String archivePath,
                         String downloadPath,
                         String[] additionalStagingPaths,
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
        if (productionType == null) {
            throw new NullPointerException("productionType");
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
        this.productionType = productionType;
        this.archivePath = archivePath;
        this.downloadPath = downloadPath;
        this.additionalStagingPaths = additionalStagingPaths;
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

    public String getProductionType() {
        return productionType;
    }

    public String getInventoryPath() {
        return archivePath;
    }

    public String getDownloadPath() {
        return downloadPath;
    }

    public String[] getAdditionalStagingPaths() {
        return additionalStagingPaths;
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
