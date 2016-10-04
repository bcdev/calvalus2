package com.bc.calvalus.wps.wpsoperations;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.localprocess.SubsettingProcess;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import com.bc.wps.api.exceptions.MissingParameterValueException;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ProcessBriefType;
import com.bc.wps.api.schema.ResponseDocumentType;
import com.bc.wps.api.schema.ResponseFormType;
import com.bc.wps.api.utils.WpsTypeConverter;
import org.esa.snap.core.gpf.GPF;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusExecuteOperation extends WpsOperation {

    private WpsRequestContext context;

    public CalvalusExecuteOperation(WpsRequestContext context) throws IOException {
        super(context);
        this.context = context;
    }

    public ExecuteResponse execute(Execute executeRequest)
                throws InvalidProcessorIdException, MissingParameterValueException, InvalidParameterValueException,
                       JAXBException, IOException, WpsProductionException, WpsStagingException, ProductionException,
                       WpsResultProductException {
        ProcessBriefType processBriefType = getProcessBriefType(executeRequest);
        ResponseFormType responseFormType = executeRequest.getResponseForm();
        ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();
        boolean isAsynchronous = responseDocumentType.isStatus();
        boolean isLineage = responseDocumentType.isLineage();
        String processId = executeRequest.getIdentifier().getValue();

        if (processId.equals("urbantep-local~1.0~Subset")) {
            GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            SubsettingProcess utepProcess = new SubsettingProcess();
            if (isAsynchronous) {
                LocalProductionStatus status = localFacade.orderProductionAsynchronous(executeRequest);
                ExecuteResponse asyncExecuteResponse;
                if (isLineage) {
                    asyncExecuteResponse = utepProcess.createLineageAsyncExecuteResponse(status, executeRequest, context.getServerContext());
                } else {
                    asyncExecuteResponse = executeResponse.getAcceptedResponse(status.getJobId(), context.getServerContext());
                }
                asyncExecuteResponse.setProcess(processBriefType);
                return asyncExecuteResponse;
            } else {
                LocalProductionStatus status = localFacade.orderProductionSynchronous(executeRequest);
                ExecuteResponse syncExecuteResponse;
                if (!ProcessState.COMPLETED.toString().equals(status.getState())) {
                    syncExecuteResponse = executeResponse.getFailedResponse(status.getMessage());
                } else if (isLineage) {
                    syncExecuteResponse = utepProcess.createLineageSyncExecuteResponse(status, executeRequest);
                } else {
                    syncExecuteResponse = executeResponse.getSuccessfulResponse(status.getResultUrls(), new Date());
                }
                syncExecuteResponse.setProcess(processBriefType);
                return syncExecuteResponse;
            }
        } else {
            if (isAsynchronous) {
                LocalProductionStatus status = calvalusFacade.orderProductionAsynchronous(executeRequest);
                String jobId = status.getJobId();
                ExecuteResponse asyncExecuteResponse = createAsyncExecuteResponse(executeRequest, isLineage, jobId);
                asyncExecuteResponse.setProcess(processBriefType);
                return asyncExecuteResponse;
            } else {
                LocalProductionStatus status = calvalusFacade.orderProductionSynchronous(executeRequest);
                String jobId = status.getJobId();
                ExecuteResponse syncExecuteResponse = createSyncExecuteResponse(executeRequest, isLineage, jobId);
                syncExecuteResponse.setProcess(processBriefType);
                return syncExecuteResponse;
            }
        }
    }

    ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, boolean isLineage, String productionId) {
        if (isLineage) {
            CalvalusExecuteResponseConverter executeAcceptedResponse = new CalvalusExecuteResponseConverter();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeAcceptedResponse.getAcceptedWithLineageResponse(productionId, executeRequest.getDataInputs(),
                                                                          outputType, context.getServerContext());
        } else {
            CalvalusExecuteResponseConverter executeAcceptedResponse = new CalvalusExecuteResponseConverter();
            return executeAcceptedResponse.getAcceptedResponse(productionId, context.getServerContext());
        }
    }

    ExecuteResponse createSyncExecuteResponse(Execute executeRequest, boolean isLineage, String jobId)
                throws IOException, ProductionException, WpsResultProductException {
        Production production = calvalusFacade.getProduction(jobId);
        List<String> productResultUrls = calvalusFacade.getProductResultUrls(jobId);
        WorkflowItem workflowItem = production.getWorkflow();
        if (isLineage) {
            CalvalusExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeSuccessfulResponse.getSuccessfulWithLineageResponse(productResultUrls, executeRequest.getDataInputs(), outputType);
        } else {
            CalvalusExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
            return executeSuccessfulResponse.getSuccessfulResponse(productResultUrls, workflowItem.getStopTime());
        }
    }

    private ProcessBriefType getProcessBriefType(Execute executeRequest) throws InvalidProcessorIdException {
        ProcessBriefType processBriefType = new ProcessBriefType();
        processBriefType.setIdentifier(executeRequest.getIdentifier());
        processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(executeRequest.getIdentifier().getValue()));
        ProcessorNameConverter parser = new ProcessorNameConverter(executeRequest.getIdentifier().getValue());
        processBriefType.setProcessVersion(parser.getBundleVersion());
        return processBriefType;
    }
}
