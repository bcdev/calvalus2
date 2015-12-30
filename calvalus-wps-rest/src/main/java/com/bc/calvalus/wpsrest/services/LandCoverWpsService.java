package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

/**
 * @author hans
 */
public class LandCoverWpsService extends WpsServiceProvider {

    public LandCoverWpsService(WpsMetadata wpsMetadata) {
        super(wpsMetadata);
    }

    @Override
    public String getCapabilities() {
        return null;
    }

    @Override
    public String describeProcess(String processorId) {
        return null;
    }

    @Override
    public String doExecute(Execute execute, String processorId) {
        return null;
    }

    @Override
    public String getStatus(String jobId) {
        return null;
    }
}
