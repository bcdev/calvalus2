package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import com.bc.calvalus.wpsrest.wpsoperations.describeprocess.AbstractDescribeProcessOperation;
import com.bc.calvalus.wpsrest.wpsoperations.describeprocess.CalvalusDescribeProcessOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getcapabilities.AbstractGetCapabilitiesOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getcapabilities.CalvalusGetCapabilitiesOperation;

/**
 * @author hans
 */
public class CalvalusWpsService extends WpsServiceProvider {

    public CalvalusWpsService(WpsMetadata wpsMetadata) {
        super(wpsMetadata);
    }

    @Override
    public String getCapabilities() {
        AbstractGetCapabilitiesOperation getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(wpsMetadata);
        return getCapabilitiesOperation.getCapabilities();
    }

    @Override
    public String describeProcess(String processorId) {
        AbstractDescribeProcessOperation describeProcessService = new CalvalusDescribeProcessOperation();
        return describeProcessService.describeProcess(wpsMetadata, processorId);
    }

    @Override
    public String doExecute(Execute execute, String processorId) {
        ExecuteService executeService = new ExecuteService();
        return executeService.execute(execute, wpsMetadata, processorId);
    }

    @Override
    public String getStatus(String jobId) {
        GetStatusService getStatusService = new GetStatusService();
        return getStatusService.getStatus(wpsMetadata, jobId);
    }
}
