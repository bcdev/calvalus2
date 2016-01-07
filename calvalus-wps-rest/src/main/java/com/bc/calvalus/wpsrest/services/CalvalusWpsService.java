package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import com.bc.calvalus.wpsrest.wpsoperations.describeprocess.AbstractDescribeProcessOperation;
import com.bc.calvalus.wpsrest.wpsoperations.describeprocess.CalvalusDescribeProcessOperation;
import com.bc.calvalus.wpsrest.wpsoperations.execute.AbstractExecuteOperation;
import com.bc.calvalus.wpsrest.wpsoperations.execute.CalvalusExecuteOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getcapabilities.AbstractGetCapabilitiesOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getcapabilities.CalvalusGetCapabilitiesOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getstatus.AbstractGetStatusOperation;
import com.bc.calvalus.wpsrest.wpsoperations.getstatus.CalvalusGetStatusOperation;

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
        AbstractDescribeProcessOperation describeProcessOperation = new CalvalusDescribeProcessOperation();
        return describeProcessOperation.describeProcess(wpsMetadata, processorId);
    }

    @Override
    public String doExecute(Execute execute, String processorId) {
        AbstractExecuteOperation executeOperation = new CalvalusExecuteOperation();
        return executeOperation.execute(execute, wpsMetadata, processorId);
    }

    @Override
    public String getStatus(String jobId) {
        AbstractGetStatusOperation getStatusOperation = new CalvalusGetStatusOperation(wpsMetadata, jobId);
        return getStatusOperation.getStatus();
    }
}
