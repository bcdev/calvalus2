package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.exception.WpsMissingParameterValueException;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.ResponseDocumentType;
import com.bc.calvalus.wpsrest.jaxb.ResponseFormType;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteAcceptedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteSuccessfulResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles all the Execute requests.
 *
 * @author hans
 */
public class ExecuteService {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public String execute(Execute executeRequest, WpsMetadata wpsMetadata, String processorId) {
        StringWriter stringWriter = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();
        ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
        try {
            ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);
            CalvalusHelper calvalusHelper = new CalvalusHelper(servletRequestWrapper);

            ProcessorNameParser parser = new ProcessorNameParser(processorId);
            Processor processor = calvalusHelper.getProcessor(parser);
            CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, processor,
                                                                           calvalusHelper.getProductSets());

            ProductionRequest request = new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                                              servletRequestWrapper.getUserName(),
                                                              calvalusDataInputs.getInputMapFormatted());

            ResponseFormType responseFormType = executeRequest.getResponseForm();
            ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();


            if (responseDocumentType.isStatus()) {

                Production production = calvalusHelper.orderProductionAsynchronous(request);

                ExecuteResponse executeResponse = createAsyncExecuteResponse(executeRequest, servletRequestWrapper,
                                                                             responseDocumentType, production.getId());

                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            } else {
                Production production = calvalusHelper.orderProductionSynchronous(request);
                calvalusHelper.stageProduction(production);
                calvalusHelper.observeStagingStatus(production);
                List<String> productResultUrls = calvalusHelper.getProductResultUrls(production);

                ExecuteResponse executeResponse = createSyncExecuteResponse(executeRequest, responseDocumentType, productResultUrls);

                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            }

        } catch (WpsMissingParameterValueException exception) {
            LOG.log(Level.SEVERE, "A WpsMissingParameterValueException has been caught.", exception);
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            try {
                jaxbHelper.marshal(exceptionResponse.getMissingParameterExceptionResponse(exception, ""), stringWriter);
            } catch (JAXBException jaxbException) {
                LOG.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
            }
            return stringWriter.toString();
        } catch (InterruptedException | IOException | JAXBException | DatatypeConfigurationException | ProductionException exception) {
            LOG.log(Level.SEVERE, "Unable to process an Execute request.", exception);
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            try {
                jaxbHelper.marshal(exceptionResponse.getGeneralExceptionResponse(exception), stringWriter);
            } catch (JAXBException jaxbException) {
                LOG.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
            }
            return stringWriter.toString();
        }
    }

    private ExecuteResponse createSyncExecuteResponse(Execute execute, ResponseDocumentType responseDocumentType, List<String> productResultUrls) throws DatatypeConfigurationException {
        ExecuteResponse executeResponse;
        if (responseDocumentType.isLineage()) {
            ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
            List<DocumentOutputDefinitionType> outputType = execute.getResponseForm().getResponseDocument().getOutput();
            executeResponse = executeSuccessfulResponse.getExecuteResponseWithLineage(productResultUrls, execute.getDataInputs(), outputType);
        } else {
            ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
            executeResponse = executeSuccessfulResponse.getExecuteResponse(productResultUrls);
        }
        return executeResponse;
    }

    private ExecuteResponse createAsyncExecuteResponse(Execute execute,
                                                       ServletRequestWrapper servletRequestWrapper,
                                                       ResponseDocumentType responseDocumentType,
                                                       String productionId)
                throws DatatypeConfigurationException {
        ExecuteResponse executeResponse;
        if (responseDocumentType.isLineage()) {
            ExecuteAcceptedResponse executeAcceptedResponse = new ExecuteAcceptedResponse();
            List<DocumentOutputDefinitionType> outputType = execute.getResponseForm().getResponseDocument().getOutput();
            executeResponse = executeAcceptedResponse.getExecuteResponseWithLineage(productionId, execute.getDataInputs(), outputType, servletRequestWrapper);
        } else {

            ExecuteAcceptedResponse executeAcceptedResponse = new ExecuteAcceptedResponse();
            executeResponse = executeAcceptedResponse.getExecuteResponse(productionId, servletRequestWrapper);
        }
        return executeResponse;
    }
}
