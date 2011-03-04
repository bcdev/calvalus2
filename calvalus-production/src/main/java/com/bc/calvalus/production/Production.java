package com.bc.calvalus.production;


/**
* Information about a production.
*
* @author Norman
*/
public class Production {
    private String id;
    private String name;
    private String outputUrl;
    private ProductionStatus processingStatus;
    private ProductionStatus stagingStatus;

    public Production(String id, String name) {
        if (id == null) {
            throw new NullPointerException("id");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        this.id = id;
        this.name = name;
        this.processingStatus = new ProductionStatus();
        this.stagingStatus = new ProductionStatus();
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

    public void setOutputUrl(String outputUrl) {
        this.outputUrl = outputUrl;
    }

    public ProductionStatus getProcessingStatus() {
        return processingStatus;
    }

    public void setProcessingStatus(ProductionStatus processingStatus) {
        this.processingStatus = processingStatus;
    }

    public ProductionStatus getStagingStatus() {
        return stagingStatus;
    }

    public void setStagingStatus(ProductionStatus stagingStatus) {
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
