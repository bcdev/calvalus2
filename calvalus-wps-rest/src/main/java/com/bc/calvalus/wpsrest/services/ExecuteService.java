package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.WpsException;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.LanguageStringType;
import com.bc.calvalus.wpsrest.jaxb.ProcessBriefType;
import com.bc.calvalus.wpsrest.jaxb.ResponseDocumentType;
import com.bc.calvalus.wpsrest.jaxb.ResponseFormType;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteAcceptedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteSuccessfulResponse;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

/**
 * Created by hans on 21/08/2015.
 */
@Path("/Execute")
public class ExecuteService {

    @POST
    @Path("{processorId}")
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    public String execute(Execute execute, String userName, @PathParam("processorId") String processorId) {
        StringWriter stringWriter = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();
        try {
            ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(execute);
            CalvalusHelper calvalusHelper = new CalvalusHelper(userName);

            ProcessorNameParser parser = new ProcessorNameParser(processorId);
            Processor processor = calvalusHelper.getProcessor(parser);
            CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, processor);

            ProductionRequest request = new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                                              userName,
                                                              calvalusDataInputs.getInputMapFormatted());

            ResponseFormType responseFormType = execute.getResponseForm();
            ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();

            if (responseDocumentType.isStatus()) {

                Production production = calvalusHelper.orderProductionAsynchronous(request);

                ExecuteResponse executeResponse;
                if (responseDocumentType.isLineage()) {
                    ExecuteAcceptedResponse executeAcceptedResponse = new ExecuteAcceptedResponse();
                    List<DocumentOutputDefinitionType> outputType = execute.getResponseForm().getResponseDocument().getOutput();
                    executeResponse = executeAcceptedResponse.getExecuteResponseWithLineage(production.getId(), execute.getDataInputs(), outputType);
                } else {

                    ExecuteAcceptedResponse executeAcceptedResponse = new ExecuteAcceptedResponse();
                    executeResponse = executeAcceptedResponse.getExecuteResponse(production.getId());
                }

                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            } else {
                Production production = calvalusHelper.orderProductionSynchronous(request);
                calvalusHelper.stageProduction(calvalusHelper.getProductionService(), production);
                calvalusHelper.observeStagingStatus(calvalusHelper.getProductionService(), production);
                List<String> productResultUrls = calvalusHelper.getProductResultUrls(production);

                ExecuteResponse executeResponse;
                if (responseDocumentType.isLineage()) {
                    ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
                    List<DocumentOutputDefinitionType> outputType = execute.getResponseForm().getResponseDocument().getOutput();
                    executeResponse = executeSuccessfulResponse.getExecuteResponseWithLineage(productResultUrls, execute.getDataInputs(), outputType);
                } else {
                    ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
                    executeResponse = executeSuccessfulResponse.getExecuteResponse(productResultUrls);
                }

                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            }

        } catch (InterruptedException | IOException | JAXBException | DatatypeConfigurationException | ProductionException | WpsException exception) {
            exception.printStackTrace();
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            try {
                jaxbHelper.marshal(exceptionResponse.getGeneralExceptionResponse(exception), stringWriter);
            } catch (JAXBException e) {
                e.printStackTrace();
            }
            return stringWriter.toString();
        }
    }
}
