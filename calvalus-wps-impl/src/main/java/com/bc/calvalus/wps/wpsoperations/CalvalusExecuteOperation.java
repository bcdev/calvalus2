package com.bc.calvalus.wps.wpsoperations;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.calvalusfacade.CalvalusProcessor;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameParser;
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

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusExecuteOperation {

    private WpsRequestContext context;

    public CalvalusExecuteOperation(WpsRequestContext context) {
        this.context = context;
    }

    public ExecuteResponse execute(Execute executeRequest)
                throws InterruptedException, InvalidProcessorIdException, JAXBException,
                       ProductionException, IOException, InvalidParameterValueException, MissingParameterValueException {
        ProcessBriefType processBriefType = getProcessBriefType(executeRequest);
        ResponseFormType responseFormType = executeRequest.getResponseForm();
        ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();
        boolean isAsynchronous = responseDocumentType.isStatus();
        boolean isLineage = responseDocumentType.isLineage();
        String processId = executeRequest.getIdentifier().getValue();

        if (isAsynchronous) {
            String jobId = processAsync(executeRequest, processId);
            ExecuteResponse asyncExecuteResponse = createAsyncExecuteResponse(executeRequest, isLineage, jobId);
            asyncExecuteResponse.setProcess(processBriefType);
            return asyncExecuteResponse;
        } else {
            String jobId = processSync(executeRequest, processId);
            ExecuteResponse syncExecuteResponse = createSyncExecuteResponse(executeRequest, isLineage, jobId);
            syncExecuteResponse.setProcess(processBriefType);
            return syncExecuteResponse;
        }
    }

    String processSync(Execute executeRequest, String processorId)
                throws IOException, ProductionException, InvalidProcessorIdException,
                       JAXBException, InterruptedException, InvalidParameterValueException, MissingParameterValueException {
        CalvalusFacade calvalusFacade = new CalvalusFacade(context);
        ProductionRequest request = createProductionRequest(executeRequest, processorId, calvalusFacade);

        Production production = calvalusFacade.orderProductionSynchronous(request);
        calvalusFacade.stageProduction(production);
        calvalusFacade.observeStagingStatus(production);
        return production.getId();
    }

    String processAsync(Execute executeRequest, String processorId)
                throws IOException, ProductionException, InvalidProcessorIdException, JAXBException, InvalidParameterValueException, MissingParameterValueException {
        CalvalusFacade calvalusFacade = new CalvalusFacade(context);
        ProductionRequest request = createProductionRequest(executeRequest, processorId, calvalusFacade);

        Production production = calvalusFacade.orderProductionAsynchronous(request);
        return production.getId();
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
                throws IOException, ProductionException {
        CalvalusFacade calvalusFacade = new CalvalusFacade(context);
        ProductionService productionService = calvalusFacade.getServices().getProductionService();
        Production production = productionService.getProduction(jobId);
        List<String> productResultUrls = calvalusFacade.getProductResultUrls(production);
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

    private ProductionRequest createProductionRequest(Execute executeRequest, String processorId,
                                                      CalvalusFacade calvalusFacade)
                throws JAXBException, IOException, ProductionException, InvalidProcessorIdException,
                       InvalidParameterValueException, MissingParameterValueException {
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);

        ProcessorNameParser parser = new ProcessorNameParser(processorId);
        CalvalusProcessor calvalusProcessor = calvalusFacade.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, calvalusProcessor,
                                                                       calvalusFacade.getProductSets());

        return new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                     context.getUserName(),
                                     calvalusDataInputs.getInputMapFormatted());
    }

    private ProcessBriefType getProcessBriefType(Execute executeRequest) throws InvalidProcessorIdException {
        ProcessBriefType processBriefType = new ProcessBriefType();
        processBriefType.setIdentifier(executeRequest.getIdentifier());
        processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(executeRequest.getIdentifier().getValue()));
        ProcessorNameParser parser = new ProcessorNameParser(executeRequest.getIdentifier().getValue());
        processBriefType.setProcessVersion(parser.getBundleVersion());
        return processBriefType;
    }
}
