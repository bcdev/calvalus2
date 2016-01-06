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
import com.bc.calvalus.wpsrest.responses.AbstractExecuteResponse;
import com.bc.calvalus.wpsrest.responses.CalvalusExecuteResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteAcceptedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteSuccessfulResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusExecuteOperation extends AbstractExecuteOperation {

    @Override
    public List<String> processSync(Execute executeRequest, String processorId, WpsMetadata wpsMetadata)
            throws IOException, ProductionException, JAXBException, InterruptedException {
        ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
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

    @Override
    public String processAsync(Execute executeRequest, String processorId, WpsMetadata wpsMetadata)
            throws IOException, ProductionException, JAXBException {
        ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
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
    public ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, WpsMetadata wpsMetadata,
                                                      ResponseDocumentType responseDocumentType, String productionId)
            throws DatatypeConfigurationException {
        if (responseDocumentType.isLineage()) {
            AbstractExecuteResponse executeAcceptedResponse = new CalvalusExecuteResponse();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeAcceptedResponse.getAcceptedWithLineageResponse(productionId, executeRequest.getDataInputs(),
                    outputType, wpsMetadata);
        } else {
            AbstractExecuteResponse executeAcceptedResponse = new CalvalusExecuteResponse();
            return executeAcceptedResponse.getAcceptedResponse(productionId, wpsMetadata);
        }
    }

    @Override
    public ExecuteResponse createSyncExecuteResponse(Execute executeRequest, ResponseDocumentType responseDocumentType,
                                                     List<String> productResultUrls)
            throws DatatypeConfigurationException {
        if (responseDocumentType.isLineage()) {
            AbstractExecuteResponse executeSuccessfulResponse = new CalvalusExecuteResponse();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeSuccessfulResponse.getSuccessfulWithLineageResponse(productResultUrls, executeRequest.getDataInputs(), outputType);
        } else {
            AbstractExecuteResponse executeSuccessfulResponse = new CalvalusExecuteResponse();
            return executeSuccessfulResponse.getSuccessfulResponse(productResultUrls);
        }
    }

    @Override
    public Logger getLogger() {
        return CalvalusLogger.getLogger();
    }
}
