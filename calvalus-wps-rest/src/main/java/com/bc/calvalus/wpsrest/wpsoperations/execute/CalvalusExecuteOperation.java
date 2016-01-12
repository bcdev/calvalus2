package com.bc.calvalus.wpsrest.wpsoperations.execute;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wpsrest.CalvalusProcessor;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.WpsRuntimeException;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.AbstractExecuteResponseConverter;
import com.bc.calvalus.wpsrest.responses.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusExecuteOperation extends AbstractExecuteOperation {

    public CalvalusExecuteOperation(Execute executeRequest, WpsMetadata wpsMetadata, String processId) {
        super(executeRequest, wpsMetadata, processId);
    }

    @Override
    public List<String> processSync(Execute executeRequest, String processorId, WpsMetadata wpsMetadata) {
        try {
            ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
            CalvalusFacade calvalusFacade = new CalvalusFacade(servletRequestWrapper);
            ProductionRequest request = createProductionRequest(executeRequest, processorId, servletRequestWrapper, calvalusFacade);

            Production production = calvalusFacade.orderProductionSynchronous(request);
            calvalusFacade.stageProduction(production);
            calvalusFacade.observeStagingStatus(production);
            return calvalusFacade.getProductResultUrls(production);
        } catch (InterruptedException | IOException | JAXBException | ProductionException exception) {
            throw new WpsRuntimeException("Unable to process the request synchronously", exception);
        }
    }

    @Override
    public String processAsync(Execute executeRequest, String processorId, WpsMetadata wpsMetadata) {
        try {
            ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
            CalvalusFacade calvalusFacade = new CalvalusFacade(servletRequestWrapper);
            ProductionRequest request = createProductionRequest(executeRequest, processorId, servletRequestWrapper, calvalusFacade);

            Production production = calvalusFacade.orderProductionAsynchronous(request);
            return production.getId();
        } catch (IOException | JAXBException | ProductionException exception) {
            throw new WpsRuntimeException("Unable to process the request asynchronously", exception);
        }
    }

    @Override
    public ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, WpsMetadata wpsMetadata, boolean isLineage, String productionId) {
        if (isLineage) {
            AbstractExecuteResponseConverter executeAcceptedResponse = new CalvalusExecuteResponseConverter();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeAcceptedResponse.getAcceptedWithLineageResponse(productionId, executeRequest.getDataInputs(),
                                                                          outputType, wpsMetadata);
        } else {
            AbstractExecuteResponseConverter executeAcceptedResponse = new CalvalusExecuteResponseConverter();
            return executeAcceptedResponse.getAcceptedResponse(productionId, wpsMetadata);
        }
    }

    @Override
    public ExecuteResponse createSyncExecuteResponse(Execute executeRequest, boolean isLineage, List<String> productResultUrls) {
        if (isLineage) {
            AbstractExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeSuccessfulResponse.getSuccessfulWithLineageResponse(productResultUrls, executeRequest.getDataInputs(), outputType);
        } else {
            AbstractExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
            return executeSuccessfulResponse.getSuccessfulResponse(productResultUrls);
        }
    }

    @Override
    public Logger getLogger() {
        return CalvalusLogger.getLogger();
    }

    private ProductionRequest createProductionRequest(Execute executeRequest, String processorId,
                                                      ServletRequestWrapper servletRequestWrapper,
                                                      CalvalusFacade calvalusFacade)
                throws JAXBException, IOException, ProductionException {
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);

        ProcessorNameParser parser = new ProcessorNameParser(processorId);
        CalvalusProcessor calvalusProcessor = calvalusFacade.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, calvalusProcessor,
                                                                       calvalusFacade.getProductSets());

        return new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                     servletRequestWrapper.getUserName(),
                                     calvalusDataInputs.getInputMapFormatted());
    }
}
