package com.bc.calvalus.wps.wpsoperations;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.wps.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wps.calvalusfacade.CalvalusWpsProcessStatus;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.JobNotFoundException;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.SqlStoreException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.localprocess.GpfProductionService;
import com.bc.calvalus.wps.localprocess.LocalJob;
import com.bc.calvalus.wps.localprocess.LocalProductionService;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.localprocess.WpsProcessStatus;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ProcessBriefType;
import com.bc.wps.api.utils.WpsTypeConverter;

import java.io.IOException;
import java.util.Map;

/**
 * @author hans
 */
public class CalvalusGetStatusOperation extends WpsOperation {

    public CalvalusGetStatusOperation(WpsRequestContext context) throws IOException {
        super(context);
    }

    public ExecuteResponse getStatus(String jobId) throws JobNotFoundException, InvalidProcessorIdException, SqlStoreException {
        // to match urban1-20160919_160202392, hans-20150919_999999999, etc.
        String localJobIdRegex = ".*-((\\d{4}((0[13578]|1[02])(0[1-9]|[12]\\d|3[01])|(0[13456789]|1[012])(0[1-9]|" +
                                 "[12]\\d|30)|02(0[1-9]|1\\d|2[0-8])))|(\\d{2}[02468][048]|\\d{2}[13579][26])0229){0,8}_.*";
        if (jobId.matches(localJobIdRegex)) {
            try {
                return getLocalProcessExecuteResponse(jobId);
            } catch (JobNotFoundException _) {}
        }
        return getCalvalusExecuteResponse(jobId);
    }

    private ExecuteResponse getCalvalusExecuteResponse(String jobId) throws JobNotFoundException {
        ExecuteResponse executeResponse;
        Production production;
        WpsProcessStatus processStatus;
        ProcessBriefType processBriefType = new ProcessBriefType();
        try {
            production = calvalusFacade.getProduction(jobId);
            if (production == null) {
                throw new JobNotFoundException("JobId " + jobId);
            }
            ProductionRequest productionRequest = production.getProductionRequest();
            ProcessorNameConverter processorConverter = new ProcessorNameConverter(productionRequest.getString(PROCESSOR_BUNDLE_NAME.getIdentifier()),
                                                                                   productionRequest.getString(PROCESSOR_BUNDLE_VERSION.getIdentifier()),
                                                                                   productionRequest.getString(PROCESSOR_NAME.getIdentifier(), "ra")); // TODO find better way to handle RA

            String processId = processorConverter.getProcessorIdentifier();
            processBriefType.setIdentifier(WpsTypeConverter.str2CodeType(processId));
            processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(processId));
            processBriefType.setProcessVersion(processorConverter.getBundleVersion());
            processStatus = new CalvalusWpsProcessStatus(production, calvalusFacade.getProductResultUrls(jobId));
            if (ProcessState.COMPLETED.toString().equals(processStatus.getState())) {
                Map<String, String> config = ProductionServiceConfig.loadConfig(CalvalusProductionService.getConfigFile(), null);
                if(Boolean.valueOf(config.get("calvalus.generate.metadata"))){
                calvalusFacade.generateProductMetadata(jobId);
                }
                executeResponse = getExecuteSuccessfulResponse(processStatus);
            } else if (processStatus.isDone()) {
                executeResponse = getExecuteFailedResponse(processStatus);
            } else {
                executeResponse = getExecuteInProgressResponse(processStatus);
            }
        } catch (ProductionException | IOException | WpsResultProductException | ProductMetadataException exception) {
            throw new JobNotFoundException(exception, "JobId " + jobId);
        }
        executeResponse.setProcess(processBriefType);
        return executeResponse;
    }

    private ExecuteResponse getLocalProcessExecuteResponse(String jobId)
                throws JobNotFoundException, InvalidProcessorIdException, SqlStoreException {
        ExecuteResponse executeResponse;
        LocalProductionService productionService = GpfProductionService.getProductionServiceSingleton();
        LocalJob job = productionService.getJob(jobId);
        if (job == null) {
            throw new JobNotFoundException("JobId " + jobId);
        }
        LocalProductionStatus status = job.getStatus();
        String processId = (String) job.getParameters().get("processId");
        ProcessorNameConverter processorNameConverter = new ProcessorNameConverter(processId);
        ProcessBriefType processBriefType = new ProcessBriefType();
        processBriefType.setIdentifier(WpsTypeConverter.str2CodeType(processId));
        processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(processId));
        processBriefType.setProcessVersion(processorNameConverter.getBundleVersion());
        if (ProcessState.COMPLETED.toString().equals(status.getState())) {
            executeResponse = getExecuteSuccessfulResponse(status);
        } else if (ProcessState.ERROR.toString().equals(status.getState())) {
            executeResponse = getExecuteFailedResponse(status);
        } else {
            executeResponse = getExecuteInProgressResponse(status);
        }
        executeResponse.setProcess(processBriefType);
        CalvalusLogger.getLogger().info("status inquiry for job " + jobId + " returns " + status.getState());
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
