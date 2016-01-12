package com.bc.calvalus.wpsrest.wpsoperations.getstatus;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.JobNotFoundException;
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

    public CalvalusGetStatusOperation(WpsMetadata wpsMetadata, String jobId) {
        super(wpsMetadata, jobId);
    }

    @Override
    public ExecuteResponse getExecuteAcceptedResponse() throws JobNotFoundException {
        try {
            Production production = getProduction();
            ProcessStatus processingStatus = production.getProcessingStatus();
            AbstractExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getStartedResponse(processingStatus.getState().toString(), 100 * processingStatus.getProgress());
        } catch (IOException | ProductionException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    @Override
    public ExecuteResponse getExecuteFailedResponse() throws JobNotFoundException {
        try {
            Production production = getProduction();
            AbstractExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getFailedResponse(production.getProcessingStatus().getMessage());
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    @Override
    public ExecuteResponse getExecuteSuccessfulResponse() throws JobNotFoundException {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(wpsMetadata.getServletRequestWrapper());
            ProductionService productionService = calvalusFacade.getProductionService();
            Production production = productionService.getProduction(jobId);
            List<String> productResultUrls = calvalusFacade.getProductResultUrls(production);
            AbstractExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getSuccessfulResponse(productResultUrls);
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    @Override
    public boolean isProductionJobFinishedAndSuccessful() throws JobNotFoundException {
        try {
            Production production = getProduction();
            return production.getStagingStatus().getState() == ProcessState.COMPLETED;
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    @Override
    public boolean isProductionJobFinishedAndFailed() throws JobNotFoundException {
        try {
            Production production = getProduction();
            return production.getProcessingStatus().getState().isDone();
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    @Override
    public Logger getLogger() {
        return CalvalusLogger.getLogger();
    }

    private Production getProduction() throws IOException, ProductionException {
        CalvalusFacade calvalusFacade = new CalvalusFacade(wpsMetadata.getServletRequestWrapper());
        ProductionService productionService = calvalusFacade.getProductionService();
        return productionService.getProduction(jobId);
    }
}
