package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.jaxb.Execute;

/**
 * @author hans
 */
public class CalvalusWpsService extends AbstractWpsService {

    public CalvalusWpsService(ServletRequestWrapper servletRequestWrapper) {
        super(servletRequestWrapper);
    }

    @Override
    public String getCapabilities() {
        GetCapabilitiesService getCapabilitiesService = new GetCapabilitiesService();
        return getCapabilitiesService.getCapabilities(servletRequestWrapper);
    }

    @Override
    public String describeProcess(String processorId, String version) {
        DescribeProcessService describeProcessService = new DescribeProcessService();
        return describeProcessService.describeProcess(servletRequestWrapper, processorId);
    }

    @Override
    public String doExecute(Execute execute, String processorId) {
        ExecuteService executeService = new ExecuteService();
        return executeService.execute(execute, servletRequestWrapper, processorId);
    }

    @Override
    public String getStatus(String jobId) {
        GetStatusService getStatusService = new GetStatusService();
        return getStatusService.getStatus(servletRequestWrapper, jobId);
    }
}
