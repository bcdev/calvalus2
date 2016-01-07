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
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.AbstractExecuteResponse;
import com.bc.calvalus.wpsrest.responses.CalvalusExecuteResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusExecuteOperation extends AbstractExecuteOperation {

    @Override
    public List<String> processSync(Execute executeRequest, String processorId, WpsMetadata wpsMetadata) {
        try {
            ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
            CalvalusHelper calvalusHelper = new CalvalusHelper(servletRequestWrapper);
            ProductionRequest request = createProductionRequest(executeRequest, processorId, servletRequestWrapper, calvalusHelper);

            Production production = calvalusHelper.orderProductionSynchronous(request);
            calvalusHelper.stageProduction(production);
            calvalusHelper.observeStagingStatus(production);
            return calvalusHelper.getProductResultUrls(production);
        } catch (InterruptedException | IOException | JAXBException | ProductionException exception) {
            throw new WpsException("Unable to process the request synchronously", exception);
        }
    }

    @Override
    public String processAsync(Execute executeRequest, String processorId, WpsMetadata wpsMetadata) {
        try {
            ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
            CalvalusHelper calvalusHelper = new CalvalusHelper(servletRequestWrapper);
            ProductionRequest request = createProductionRequest(executeRequest, processorId, servletRequestWrapper, calvalusHelper);

            Production production = calvalusHelper.orderProductionAsynchronous(request);
            return production.getId();
        } catch (IOException | JAXBException | ProductionException exception) {
            throw new WpsException("Unable to process the request asynchronously", exception);
        }
    }

    @Override
    public ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, WpsMetadata wpsMetadata, boolean isLineage, String productionId) {
        if (isLineage) {
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
    public ExecuteResponse createSyncExecuteResponse(Execute executeRequest, boolean isLineage, List<String> productResultUrls) {
        if (isLineage) {
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

    private ProductionRequest createProductionRequest(Execute executeRequest, String processorId,
                                                      ServletRequestWrapper servletRequestWrapper,
                                                      CalvalusHelper calvalusHelper)
                throws JAXBException, IOException, ProductionException {
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);

        ProcessorNameParser parser = new ProcessorNameParser(processorId);
        Processor processor = calvalusHelper.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, processor,
                                                                       calvalusHelper.getProductSets());

        return new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                     servletRequestWrapper.getUserName(),
                                     calvalusDataInputs.getInputMapFormatted());
    }
}
