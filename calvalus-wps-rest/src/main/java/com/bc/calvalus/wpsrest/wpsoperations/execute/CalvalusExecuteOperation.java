package com.bc.calvalus.wpsrest.wpsoperations.execute;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.ResponseDocumentType;
import com.bc.calvalus.wpsrest.responses.ExecuteAcceptedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteSuccessfulResponse;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusExecuteOperation extends AbstractExecuteOperation {

    public List<String> processSync(Execute executeRequest, String processorId, ServletRequestWrapper servletRequestWrapper) throws IOException, ProductionException, JAXBException, InterruptedException {
        CalvalusHelper calvalusHelper = new CalvalusHelper(servletRequestWrapper);
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);

        ProcessorNameParser parser = new ProcessorNameParser(processorId);
        Processor processor = calvalusHelper.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, processor,
                                                                       calvalusHelper.getProductSets());

        ProductionRequest request = new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                                          servletRequestWrapper.getUserName(),
                                                          calvalusDataInputs.getInputMapFormatted());

        Production production = calvalusHelper.orderProductionSynchronous(request);
        calvalusHelper.stageProduction(production);
        calvalusHelper.observeStagingStatus(production);
        return calvalusHelper.getProductResultUrls(production);
    }

    public String processAsync(Execute executeRequest, String processorId, ServletRequestWrapper servletRequestWrapper) throws IOException, ProductionException, JAXBException {
        CalvalusHelper calvalusHelper = new CalvalusHelper(servletRequestWrapper);
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);

        ProcessorNameParser parser = new ProcessorNameParser(processorId);
        Processor processor = calvalusHelper.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, processor,
                                                                       calvalusHelper.getProductSets());

        ProductionRequest request = new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                                          servletRequestWrapper.getUserName(),
                                                          calvalusDataInputs.getInputMapFormatted());

        Production production = calvalusHelper.orderProductionAsynchronous(request);

        return production.getId();
    }

    @Override
    public ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, ServletRequestWrapper servletRequestWrapper,
                                                      ResponseDocumentType responseDocumentType, String productionId)
                throws DatatypeConfigurationException {
        if (responseDocumentType.isLineage()) {
            ExecuteAcceptedResponse executeAcceptedResponse = new ExecuteAcceptedResponse();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeAcceptedResponse.getExecuteResponseWithLineage(productionId, executeRequest.getDataInputs(),
                                                                                    outputType, servletRequestWrapper);
        } else {
            ExecuteAcceptedResponse executeAcceptedResponse = new ExecuteAcceptedResponse();
            return executeAcceptedResponse.getExecuteResponse(productionId, servletRequestWrapper);
        }
    }

    @Override
    public ExecuteResponse createSyncExecuteResponse(Execute executeRequest, ResponseDocumentType responseDocumentType, List<String> productResultUrls) throws DatatypeConfigurationException {
        if (responseDocumentType.isLineage()) {
            ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeSuccessfulResponse.getExecuteResponseWithLineage(productResultUrls, executeRequest.getDataInputs(), outputType);
        } else {
            ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
            return executeSuccessfulResponse.getExecuteResponse(productResultUrls);
        }
    }

    @Override
    public Logger getLogger() {
        return CalvalusLogger.getLogger();
    }
}
