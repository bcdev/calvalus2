package com.bc.calvalus.wps.wpsoperations;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.calvalusfacade.CalvalusProcessor;
import com.bc.calvalus.wps.exceptions.FailedRequestException;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.responses.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameParser;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ResponseDocumentType;
import com.bc.wps.api.schema.ResponseFormType;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusExecuteOperation {

    protected WpsRequestContext context;

    public CalvalusExecuteOperation(WpsRequestContext context) {
        this.context = context;
    }

    public ExecuteResponse execute(Execute executeRequest) throws FailedRequestException {
        ResponseFormType responseFormType = executeRequest.getResponseForm();
        ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();
        boolean isAsynchronous = responseDocumentType.isStatus();
        boolean isLineage = responseDocumentType.isLineage();
        String processId = executeRequest.getIdentifier().getValue();

        if (isAsynchronous) {
            String jobId = processAsync(executeRequest, processId);
            return createAsyncExecuteResponse(executeRequest, isLineage, jobId);
        } else {
            List<String> results = processSync(executeRequest, processId);
            return createSyncExecuteResponse(executeRequest, isLineage, results);
        }
    }

    protected List<String> processSync(Execute executeRequest, String processorId) throws FailedRequestException {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(context);
            ProductionRequest request = createProductionRequest(executeRequest, processorId, calvalusFacade);

            Production production = calvalusFacade.orderProductionSynchronous(request);
            calvalusFacade.stageProduction(production);
            calvalusFacade.observeStagingStatus(production);
            return calvalusFacade.getProductResultUrls(production);
        } catch (InterruptedException | IOException | JAXBException | ProductionException | InvalidProcessorIdException exception) {
            throw new FailedRequestException("Unable to process the request synchronously", exception);
        }
    }

    protected String processAsync(Execute executeRequest, String processorId) throws FailedRequestException {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(context);
            ProductionRequest request = createProductionRequest(executeRequest, processorId, calvalusFacade);

            Production production = calvalusFacade.orderProductionAsynchronous(request);
            return production.getId();
        } catch (IOException | JAXBException | ProductionException | InvalidProcessorIdException exception) {
            throw new FailedRequestException("Unable to process the request asynchronously", exception);
        }
    }

    protected ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, boolean isLineage, String productionId) {
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

    protected ExecuteResponse createSyncExecuteResponse(Execute executeRequest, boolean isLineage, List<String> productResultUrls) {
        if (isLineage) {
            CalvalusExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeSuccessfulResponse.getSuccessfulWithLineageResponse(productResultUrls, executeRequest.getDataInputs(), outputType);
        } else {
            CalvalusExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
            return executeSuccessfulResponse.getSuccessfulResponse(productResultUrls);
        }
    }

    protected ProductionRequest createProductionRequest(Execute executeRequest, String processorId,
                                                      CalvalusFacade calvalusFacade)
                throws JAXBException, IOException, ProductionException, InvalidProcessorIdException {
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);

        ProcessorNameParser parser = new ProcessorNameParser(processorId);
        CalvalusProcessor calvalusProcessor = calvalusFacade.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, calvalusProcessor,
                                                                       calvalusFacade.getProductSets());

        return new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                     context.getUserName(),
                                     calvalusDataInputs.getInputMapFormatted());
    }
}
