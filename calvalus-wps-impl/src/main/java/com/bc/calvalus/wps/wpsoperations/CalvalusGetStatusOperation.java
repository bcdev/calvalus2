package com.bc.calvalus.wps.wpsoperations;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.exceptions.JobNotFoundException;
import com.bc.calvalus.wps.responses.CalvalusExecuteResponseConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ProcessBriefType;
import com.bc.wps.api.utils.WpsTypeConverter;

import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusGetStatusOperation {

    private WpsRequestContext context;

    public CalvalusGetStatusOperation(WpsRequestContext context) {
        this.context = context;
    }

    public ExecuteResponse getStatus(String jobId) throws JobNotFoundException {
        ExecuteResponse executeResponse;
        Production production;
        ProcessBriefType processBriefType = new ProcessBriefType();
        try{
            production = getProduction(jobId);
            String bundleName = production.getProductionRequest().getString(PROCESSOR_BUNDLE_NAME.getIdentifier());
            String bundleVersion = production.getProductionRequest().getString(PROCESSOR_BUNDLE_VERSION.getIdentifier());
            String processorName = production.getProductionRequest().getString(PROCESSOR_NAME.getIdentifier());
            String processId = bundleName.concat("~").concat(bundleVersion).concat("~").concat(processorName);
            processBriefType.setIdentifier(WpsTypeConverter.str2CodeType(processId));
            processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(processId));
            processBriefType.setProcessVersion(bundleVersion);
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }

        if (isProductionJobFinishedAndSuccessful(jobId)) {
            executeResponse = getExecuteSuccessfulResponse(jobId);
        } else if (isProductionJobFinishedAndFailed(jobId)) {
            executeResponse = getExecuteFailedResponse(jobId);
        } else {
            executeResponse = getExecuteInProgressResponse(jobId);
        }
        executeResponse.setProcess(processBriefType);
        return executeResponse;
    }

    private ExecuteResponse getExecuteInProgressResponse(String jobId) throws JobNotFoundException {
        try {
            Production production = getProduction(jobId);
            ProcessStatus processingStatus = production.getProcessingStatus();
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getStartedResponse(processingStatus.getState().toString(), 100 * processingStatus.getProgress());
        } catch (IOException | ProductionException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    private ExecuteResponse getExecuteFailedResponse(String jobId) throws JobNotFoundException {
        try {
            Production production = getProduction(jobId);
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getFailedResponse(production.getProcessingStatus().getMessage());
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    private ExecuteResponse getExecuteSuccessfulResponse(String jobId) throws JobNotFoundException {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(context);
            ProductionService productionService = calvalusFacade.getProductionService();
            Production production = productionService.getProduction(jobId);
            List<String> productResultUrls = calvalusFacade.getProductResultUrls(production);
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getSuccessfulResponse(productResultUrls);
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    private boolean isProductionJobFinishedAndSuccessful(String jobId) throws JobNotFoundException {
        try {
            Production production = getProduction(jobId);
            return production.getStagingStatus().getState() == ProcessState.COMPLETED;
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    private boolean isProductionJobFinishedAndFailed(String jobId) throws JobNotFoundException {
        try {
            Production production = getProduction(jobId);
            return production.getProcessingStatus().getState().isDone();
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.", exception);
        }
    }

    private Production getProduction(String jobId) throws IOException, ProductionException {
        CalvalusFacade calvalusFacade = new CalvalusFacade(context);
        ProductionService productionService = calvalusFacade.getProductionService();
        return productionService.getProduction(jobId);
    }
}
