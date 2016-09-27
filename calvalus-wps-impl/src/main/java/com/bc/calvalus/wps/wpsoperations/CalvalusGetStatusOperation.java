package com.bc.calvalus.wps.wpsoperations;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
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
public class CalvalusGetStatusOperation extends WpsOperation {

    public CalvalusGetStatusOperation(WpsRequestContext context) throws IOException {
        super(context);
    }

    public ExecuteResponse getStatus(String jobId) throws JobNotFoundException {
        // to match urban1-20160919_160202392, hans-20150919_999999999, etc.
        String localJobIdRegex = ".*-((\\d{4}((0[13578]|1[02])(0[1-9]|[12]\\d|3[01])|(0[13456789]|1[012])(0[1-9]|" +
                                 "[12]\\d|30)|02(0[1-9]|1\\d|2[0-8])))|(\\d{2}[02468][048]|\\d{2}[13579][26])0229){0,8}_.*";
        if (jobId.matches(localJobIdRegex)) {
            return getLocalProcessExecuteResponse(jobId);
        }
        return getCalvalusExecuteResponse(jobId);
    }

    private ExecuteResponse getCalvalusExecuteResponse(String jobId) throws JobNotFoundException {
        ExecuteResponse executeResponse;
        Production production;
        ProcessBriefType processBriefType = new ProcessBriefType();
        try {
            production = calvalusFacade.getProduction(jobId);
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

        WpsProcessStatus processStatus = new CalvalusWpsProcessStatus(production, calvalusFacade.getProductResultUrls(production));

        if (ProcessState.COMPLETED.toString().equals(processStatus.getState())) {
            executeResponse = getExecuteSuccessfulResponse(processStatus);
        } else if (processStatus.isDone()) {
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
            throw new JobNotFoundException("JobId");
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
}
