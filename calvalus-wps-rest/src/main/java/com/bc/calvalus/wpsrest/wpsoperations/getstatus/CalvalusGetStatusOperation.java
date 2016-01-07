package com.bc.calvalus.wpsrest.wpsoperations.getstatus;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.AbstractExecuteResponse;
import com.bc.calvalus.wpsrest.responses.CalvalusExecuteResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusGetStatusOperation extends AbstractGetStatusOperation {

    private CalvalusHelper calvalusHelper;
    private Production production;

    public CalvalusGetStatusOperation(WpsMetadata wpsMetadata, String jobId) {
        super(wpsMetadata, jobId);
        try {
            this.calvalusHelper = new CalvalusHelper(wpsMetadata.getServletRequestWrapper());
            production = getProduction(jobId);
        } catch (IOException | ProductionException exception) {
            throw new WpsException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    @Override
    public ExecuteResponse getExecuteAcceptedResponse() {
        ProcessStatus processingStatus = production.getProcessingStatus();
        AbstractExecuteResponse executeResponse = new CalvalusExecuteResponse();
        return executeResponse.getStartedResponse(processingStatus.getState().toString(), 100 * processingStatus.getProgress());
    }

    @Override
    public ExecuteResponse getExecuteFailedResponse() {
        AbstractExecuteResponse executeResponse = new CalvalusExecuteResponse();
        return executeResponse.getFailedResponse(production.getProcessingStatus().getMessage());
    }

    @Override
    public ExecuteResponse getExecuteSuccessfulResponse() {
        List<String> productResultUrls = calvalusHelper.getProductResultUrls(production);
        AbstractExecuteResponse executeResponse = new CalvalusExecuteResponse();
        return executeResponse.getSuccessfulResponse(productResultUrls);
    }

    @Override
    public boolean isProductionJobFinishedAndFailed() {
        return production.getProcessingStatus().getState().isDone();
    }

    @Override
    public boolean isProductionJobFinishedAndSuccessful() {
        return production.getStagingStatus().getState() == ProcessState.COMPLETED;
    }

    @Override
    public Logger getLogger() {
        return CalvalusLogger.getLogger();
    }

    private Production getProduction(String jobId) throws IOException, ProductionException {
        ProductionService productionService = calvalusHelper.getProductionService();
        return productionService.getProduction(jobId);
    }
}
