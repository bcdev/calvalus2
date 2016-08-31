package com.bc.calvalus.wps.localprocess;

import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.Execute;
import org.esa.snap.core.datamodel.Product;

import java.nio.file.Path;
import java.util.Map;

/**
 * @author hans
 */
public class ProcessBuilder {

    private String jobId;
    private Map<String, Object> parameters;
    private Product sourceProduct;
    private Path targetDirPath;
    private WpsServerContext serverContext;
    private Execute executeRequest;

    public static ProcessBuilder create(){
        return new ProcessBuilder();
    }

    public String getJobId() {
        return jobId;
    }

    public ProcessBuilder withJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public ProcessBuilder withParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
        return this;
    }

    public Product getSourceProduct() {
        return sourceProduct;
    }

    public ProcessBuilder withSourceProduct(Product sourceProduct) {
        this.sourceProduct = sourceProduct;
        return this;
    }

    public Path getTargetDirPath() {
        return targetDirPath;
    }

    public ProcessBuilder withTargetDirPath(Path targetDirPath) {
        this.targetDirPath = targetDirPath;
        return this;
    }

    public WpsServerContext getServerContext() {
        return serverContext;
    }

    public ProcessBuilder withServerContext(WpsServerContext serverContext) {
        this.serverContext = serverContext;
        return this;
    }

    public Execute getExecuteRequest() {
        return executeRequest;
    }

    public ProcessBuilder withExecuteRequest(Execute executeRequest) {
        this.executeRequest = executeRequest;
        return this;
    }
}
