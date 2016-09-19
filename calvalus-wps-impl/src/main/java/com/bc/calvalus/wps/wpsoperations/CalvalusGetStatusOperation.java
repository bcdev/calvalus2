package com.bc.calvalus.wps.wpsoperations;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.calvalusfacade.CalvalusWpsProcessStatus;
import com.bc.calvalus.wps.exceptions.JobNotFoundException;
import com.bc.calvalus.wps.localprocess.GpfProductionService;
import com.bc.calvalus.wps.localprocess.ProductionState;
import com.bc.calvalus.wps.localprocess.WpsProcessStatus;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ProcessBriefType;
import com.bc.wps.api.utils.WpsTypeConverter;

import java.io.IOException;

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
            return getLocalProcessExecuteResponse(jobId);
        }
        return getCalvalusExecuteResponse(jobId);
    }

    private ExecuteResponse getCalvalusExecuteResponse(String jobId) throws JobNotFoundException {
        ExecuteResponse executeResponse;
        Production production;
        ProcessBriefType processBriefType = new ProcessBriefType();
        try {
            production = getProduction(jobId);
            if (production == null) {
                throw new JobNotFoundException("JobId");
            }
            ProductionRequest productionRequest = production.getProductionRequest();
            ProcessorNameConverter processorConverter = new ProcessorNameConverter(productionRequest.getString(PROCESSOR_BUNDLE_NAME.getIdentifier()),
                                                                                   productionRequest.getString(PROCESSOR_BUNDLE_VERSION.getIdentifier()),
                                                                                   productionRequest.getString(PROCESSOR_NAME.getIdentifier()));

            String processId = processorConverter.getProcessorIdentifier();
            processBriefType.setIdentifier(WpsTypeConverter.str2CodeType(processId));
            processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(processId));
            processBriefType.setProcessVersion(processorConverter.getBundleVersion());
        } catch (ProductionException | IOException exception) {
            throw new JobNotFoundException(exception, "JobId");
        }

        CalvalusFacade calvalusFacade = new CalvalusFacade(context);
        WpsProcessStatus processStatus = new CalvalusWpsProcessStatus(production, calvalusFacade.getProductResultUrls(production));

        if (isProductionJobFinishedAndSuccessful(processStatus)) {
            executeResponse = getExecuteSuccessfulResponse(processStatus);
        } else if (isProductionJobFinishedAndFailed(processStatus)) {
            executeResponse = getExecuteFailedResponse(processStatus);
        } else {
            executeResponse = getExecuteInProgressResponse(processStatus);
        }
        executeResponse.setProcess(processBriefType);
        return executeResponse;
    }

    private ExecuteResponse getLocalProcessExecuteResponse(String jobId) throws JobNotFoundException {
        ExecuteResponse executeResponse;
        ProcessBriefType processBriefType = new ProcessBriefType();
        ProcessorNameConverter processorNameConverter = new ProcessorNameConverter("local", "0.0.1", "Subset");
        String processId = processorNameConverter.getProcessorIdentifier();
        processBriefType.setIdentifier(WpsTypeConverter.str2CodeType(processId));
        processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(processId));
        processBriefType.setProcessVersion(processorNameConverter.getBundleVersion());
        WpsProcessStatus status = GpfProductionService.getProductionStatusMap().get(jobId);
        if (status != null) {
            if (ProductionState.SUCCESSFUL.toString().equals(status.getState())) {
                executeResponse = getExecuteSuccessfulResponse(status);
            } else if (ProductionState.FAILED.toString().equals(status.getState())) {
                executeResponse = getExecuteFailedResponse(status);
            } else {
                executeResponse = getExecuteInProgressResponse(status);
            }
            executeResponse.setProcess(processBriefType);
        } else {
            throw new JobNotFoundException("Unable to retrieve the job with jobId '" + jobId + "'.");
        }
        return executeResponse;
    }

    private ExecuteResponse getExecuteInProgressResponse(WpsProcessStatus status) {
        CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getStartedResponse(status.getState(), status.getProgress());
    }

    private ExecuteResponse getExecuteFailedResponse(WpsProcessStatus status) {
        CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getFailedResponse(status.getMessage());
    }

    private ExecuteResponse getExecuteSuccessfulResponse(WpsProcessStatus status) {
        CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        return executeResponse.getSuccessfulResponse(status.getResultUrls(), status.getStopTime());
    }

    private boolean isProductionJobFinishedAndSuccessful(WpsProcessStatus status) {
        return ProcessState.COMPLETED.toString().equals(status.getState());
    }

    private boolean isProductionJobFinishedAndFailed(WpsProcessStatus status) {
        return status.isDone();
    }

    private Production getProduction(String jobId) throws IOException, ProductionException {
        CalvalusFacade calvalusFacade = new CalvalusFacade(context);
        ProductionService productionService = calvalusFacade.getProductionService();
        return productionService.getProduction(jobId);
    }
}
