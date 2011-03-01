package com.bc.calvalus.production;


/**
* Information about a production.
*
* @author Norman
*/
public abstract class Production {
    String id;
    String name;
    String outputPath;
    ProductionStatus status;

    public Production(String id, String name, String outputPath, ProductionStatus status) {
        if (id == null) {
            throw new NullPointerException("id");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (status == null) {
            throw new NullPointerException("status");
        }
        this.id = id;
        this.name = name;
        this.outputPath = outputPath;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public ProductionStatus getStatus() {
        return status;
    }

    public void setStatus(ProductionStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Production{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", status=" + status +
                '}';
    }

}
