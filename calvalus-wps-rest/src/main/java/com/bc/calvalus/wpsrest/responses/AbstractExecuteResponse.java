package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;

/**
 * @author hans
 */
public abstract class AbstractExecuteResponse {

    public abstract ExecuteResponse getAcceptedResponse();

    public abstract ExecuteResponse getSuccessfulResponse();

}
