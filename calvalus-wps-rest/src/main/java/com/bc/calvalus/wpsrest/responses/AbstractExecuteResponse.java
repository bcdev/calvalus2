package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.jaxb.DataInputsType;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import java.util.List;

/**
 * @author hans
 */
public abstract class AbstractExecuteResponse {

    protected final ExecuteResponse executeResponse;

    public AbstractExecuteResponse() {
        this.executeResponse = new ExecuteResponse();
        this.executeResponse.setService("WPS");
        this.executeResponse.setVersion("1.0.0");
        this.executeResponse.setLang("en");
    }

    public abstract ExecuteResponse getAcceptedResponse(String jobId, WpsMetadata wpsMetadata);

    public abstract ExecuteResponse getAcceptedWithLineageResponse(String jobId,
                                                                   DataInputsType dataInputs,
                                                                   List<DocumentOutputDefinitionType> rawDataOutput,
                                                                   WpsMetadata wpsMetadata);

    public abstract ExecuteResponse getSuccessfulResponse(List<String> resultsUrls);

    public abstract ExecuteResponse getSuccessfulWithLineageResponse(List<String> resultsUrls,
                                                                     DataInputsType dataInputs,
                                                                     List<DocumentOutputDefinitionType> outputType);

    public abstract ExecuteResponse getFailedResponse(String exceptionMessage);

    public abstract ExecuteResponse getStartedResponse(String state, float progress);

}
