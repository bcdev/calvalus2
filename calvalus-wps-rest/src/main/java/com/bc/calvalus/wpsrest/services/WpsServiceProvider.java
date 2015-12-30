package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

/**
 * @author hans
 */
public abstract class WpsServiceProvider {

    WpsMetadata wpsMetadata;

    public WpsServiceProvider(WpsMetadata wpsMetadata) {
        this.wpsMetadata = wpsMetadata;
    }

    public abstract String getCapabilities();

    public abstract String describeProcess(String processorId);

    public abstract String doExecute(Execute execute, String processorId);

    public abstract String getStatus(String jobId);
}
