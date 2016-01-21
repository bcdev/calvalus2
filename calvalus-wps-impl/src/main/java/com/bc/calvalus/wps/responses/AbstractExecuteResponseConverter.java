package com.bc.calvalus.wps.responses;


import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.DataInputsType;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.ExecuteResponse;

import java.util.List;

/**
 * @author hans
 */
public abstract class AbstractExecuteResponseConverter {

    protected final ExecuteResponse executeResponse;

    public AbstractExecuteResponseConverter() {
        this.executeResponse = new ExecuteResponse();
        this.executeResponse.setService("WPS");
        this.executeResponse.setVersion("1.0.0");
        this.executeResponse.setLang("en");
    }

    public abstract ExecuteResponse getAcceptedResponse(String jobId, WpsServerContext context);

    public abstract ExecuteResponse getAcceptedWithLineageResponse(String jobId,
                                                                   DataInputsType dataInputs,
                                                                   List<DocumentOutputDefinitionType> rawDataOutput,
                                                                   WpsServerContext context);

    public abstract ExecuteResponse getSuccessfulResponse(List<String> resultsUrls);

    public abstract ExecuteResponse getSuccessfulWithLineageResponse(List<String> resultsUrls,
                                                                     DataInputsType dataInputs,
                                                                     List<DocumentOutputDefinitionType> outputType);

    public abstract ExecuteResponse getFailedResponse(String exceptionMessage);

    public abstract ExecuteResponse getStartedResponse(String state, float progress);

}
