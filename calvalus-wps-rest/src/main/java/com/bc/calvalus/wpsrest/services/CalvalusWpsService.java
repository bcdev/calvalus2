package com.bc.calvalus.wpsrest.services;

import static org.powermock.api.mockito.PowerMockito.mock;

import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import com.bc.calvalus.wpsrest.wpsoperations.getcapabilities.CalvalusGetCapabilitiesOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getcapabilities.GetCapabilitiesOperation;

/**
 * @author hans
 */
public class CalvalusWpsService extends WpsServiceProvider {

    public CalvalusWpsService(WpsMetadata wpsMetadata) {
        super(wpsMetadata);
    }

    @Override
    public String getCapabilities() {
        GetCapabilitiesOperation getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(wpsMetadata);
        return getCapabilitiesOperation.getCapabilities();
    }

    @Override
    public String describeProcess(String processorId) {
        DescribeProcessService describeProcessService = new DescribeProcessService();
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
