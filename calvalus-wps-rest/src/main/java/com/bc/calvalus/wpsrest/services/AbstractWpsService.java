package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.jaxb.Execute;

/**
 * @author hans
 */
public abstract class AbstractWpsService {

    ServletRequestWrapper servletRequestWrapper;

    public AbstractWpsService(ServletRequestWrapper servletRequestWrapper) {
        this.servletRequestWrapper = servletRequestWrapper;
    }

    public abstract String getCapabilities();

    public abstract String describeProcess(String processorId, String version);

    public abstract String doExecute(Execute execute, String processorId);

    public abstract String getStatus(String jobId);
}
