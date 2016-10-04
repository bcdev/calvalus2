package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;

import java.util.List;

/**
 * @author hans
 */
public class SubsettingProcess {

    public ExecuteResponse createLineageAsyncExecuteResponse(LocalProductionStatus status, Execute executeRequest, WpsServerContext serverContext) {
        CalvalusExecuteResponseConverter executeAcceptedResponse = new CalvalusExecuteResponseConverter();
        List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
        return executeAcceptedResponse.getAcceptedWithLineageResponse(status.getJobId(), executeRequest.getDataInputs(),
                                                                      outputType, serverContext);
    }

    public ExecuteResponse createLineageSyncExecuteResponse(LocalProductionStatus status, Execute executeRequest) {
        CalvalusExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
        List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
        return executeSuccessfulResponse.getSuccessfulWithLineageResponse(status.getResultUrls(), executeRequest.getDataInputs(), outputType);
    }
}
