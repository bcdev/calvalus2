package com.bc.calvalus.wpsrest.wpsoperations.getstatus;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.AbstractExecuteResponseConverter;
import com.bc.calvalus.wpsrest.responses.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusGetStatusOperation extends AbstractGetStatusOperation {

    private CalvalusFacade calvalusFacade;
    private Production production;

    public CalvalusGetStatusOperation(WpsMetadata wpsMetadata, String jobId) {
        super(wpsMetadata, jobId);
        try {
            this.calvalusFacade = new CalvalusFacade(wpsMetadata.getServletRequestWrapper());
            production = getProduction(jobId);
        } catch (IOException | ProductionException exception) {
            throw new WpsException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    @Override
    public ExecuteResponse getExecuteAcceptedResponse() {
        ProcessStatus processingStatus = production.getProcessingStatus();
        AbstractExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getStartedResponse(processingStatus.getState().toString(), 100 * processingStatus.getProgress());
    }

    @Override
    public ExecuteResponse getExecuteFailedResponse() {
        AbstractExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getFailedResponse(production.getProcessingStatus().getMessage());
    }

    @Override
    public ExecuteResponse getExecuteSuccessfulResponse() {
        List<String> productResultUrls = calvalusFacade.getProductResultUrls(production);
        AbstractExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getSuccessfulResponse(productResultUrls);
    }

    @Override
    public boolean isProductionJobFinishedAndSuccessful() {
        return production.getStagingStatus().getState() == ProcessState.COMPLETED;
    }

    @Override
    public boolean isProductionJobFinishedAndFailed() {
        return production.getProcessingStatus().getState().isDone();
    }

    @Override
    public Logger getLogger() {
        return CalvalusLogger.getLogger();
    }

    private Production getProduction(String jobId) throws IOException, ProductionException {
        ProductionService productionService = calvalusFacade.getProductionService();
        return productionService.getProduction(jobId);
    }
}
