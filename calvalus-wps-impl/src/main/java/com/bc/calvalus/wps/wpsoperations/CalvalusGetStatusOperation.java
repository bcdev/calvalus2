package com.bc.calvalus.wps.wpsoperations;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.exceptions.JobNotFoundException;
import com.bc.calvalus.wps.localprocess.GpfProductionService;
import com.bc.calvalus.wps.localprocess.ProductionState;
import com.bc.calvalus.wps.localprocess.ProductionStatus;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ProcessBriefType;
import com.bc.wps.api.utils.WpsTypeConverter;

import java.io.IOException;
import java.util.Date;
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
        if (jobId.startsWith("urban1")) {
            ExecuteResponse executeResponse;
            ProcessBriefType processBriefType = new ProcessBriefType();
            String bundleName = "local";
            String bundleVersion = "0.0.1";
            String processorName = "Subset";
            String processId = bundleName.concat("~").concat(bundleVersion).concat("~").concat(processorName);
            processBriefType.setIdentifier(WpsTypeConverter.str2CodeType(processId));
            processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(processId));
            processBriefType.setProcessVersion(bundleVersion);
            ProductionStatus status = GpfProductionService.getProductionStatusMap().get(jobId);
            if (status != null) {
                if (status.getState() == ProductionState.SUCCESSFUL) {
                    executeResponse = getLocalExecuteSuccessfulResponse(status);
                } else if (status.getState() == ProductionState.FAILED) {
                    executeResponse = getLocalExecuteFailedResponse(status);
                } else {
                    executeResponse = getLocalExecuteInProgressResponse(status);
                }
                executeResponse.setProcess(processBriefType);
            } else {
                throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.");
            }
            return executeResponse;
        }
        ExecuteResponse executeResponse;
        Production production;
        ProcessBriefType processBriefType = new ProcessBriefType();
        try {
            production = getProduction(jobId);
            if (production == null) {
                throw new JobNotFoundException("JobId");
            }
            String bundleName = production.getProductionRequest().getString(PROCESSOR_BUNDLE_NAME.getIdentifier());
            String bundleVersion = production.getProductionRequest().getString(PROCESSOR_BUNDLE_VERSION.getIdentifier());
            String processorName = production.getProductionRequest().getString(PROCESSOR_NAME.getIdentifier());
            String processId = bundleName.concat("~").concat(bundleVersion).concat("~").concat(processorName);
            processBriefType.setIdentifier(WpsTypeConverter.str2CodeType(processId));
            processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(processId));
            processBriefType.setProcessVersion(bundleVersion);
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException(exception, "JobId");
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

    private ExecuteResponse getLocalExecuteInProgressResponse(ProductionStatus status) {
        CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getStartedResponse(status.getState().toString(), status.getProgress());
    }

    private ExecuteResponse getLocalExecuteFailedResponse(ProductionStatus status) {
        CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getFailedResponse(status.getMessage());
    }

    private ExecuteResponse getLocalExecuteSuccessfulResponse(ProductionStatus status) {
        CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getSuccessfulResponse(status.getResultUrls(), new Date());
    }

    private ExecuteResponse getExecuteInProgressResponse(String jobId) throws JobNotFoundException {
        try {
            Production production = getProduction(jobId);
            ProcessStatus processingStatus = production.getProcessingStatus();
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getStartedResponse(processingStatus.getState().toString(), 100 * processingStatus.getProgress());
        } catch (IOException | ProductionException exception) {
            throw new JobNotFoundException(exception, "JobId");
        }
    }

    private ExecuteResponse getExecuteFailedResponse(String jobId) throws JobNotFoundException {
        try {
            Production production = getProduction(jobId);
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getFailedResponse(production.getProcessingStatus().getMessage());
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException(exception, "JobId");
        }
    }

    private ExecuteResponse getExecuteSuccessfulResponse(String jobId) throws JobNotFoundException {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(context);
            ProductionService productionService = calvalusFacade.getProductionService();
            Production production = productionService.getProduction(jobId);
            WorkflowItem workflowItem = production.getWorkflow();
            List<String> productResultUrls = calvalusFacade.getProductResultUrls(production);
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            return executeResponse.getSuccessfulResponse(productResultUrls, workflowItem.getStopTime());
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException(exception, "JobId");
        }
    }

    private boolean isProductionJobFinishedAndSuccessful(String jobId) throws JobNotFoundException {
        try {
            Production production = getProduction(jobId);
            return production.getStagingStatus().getState() == ProcessState.COMPLETED;
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException(exception, "JobId");
        }
    }

    private boolean isProductionJobFinishedAndFailed(String jobId) throws JobNotFoundException {
        try {
            Production production = getProduction(jobId);
            return production.getProcessingStatus().getState().isDone();
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException(exception, "JobId");
        }
    }

    private Production getProduction(String jobId) throws IOException, ProductionException {
        CalvalusFacade calvalusFacade = new CalvalusFacade(context);
        ProductionService productionService = calvalusFacade.getProductionService();
        return productionService.getProduction(jobId);
    }
}
