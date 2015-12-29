package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.jaxb.Execute;

import java.io.StringWriter;

/**
 * @author hans
 */
public class LandCoverWpsService extends AbstractWpsService {

    public LandCoverWpsService(ServletRequestWrapper servletRequestWrapper) {
        super(servletRequestWrapper);
    }

    @Override
    public String getCapabilities() {
        return null;
    }

    @Override
    public String describeProcess(String processorId, String version) {
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
