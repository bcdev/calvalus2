package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteFailedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteStartedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteSuccessfulResponse;

import javax.ws.rs.GET;
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
 * Created by hans on 24/08/2015.
 */
@Path("/Status")
public class GetStatusService {

    @GET
    @Path("{productionId}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getStatus(String userName, @PathParam("productionId") String productionId) {
        JaxbHelper jaxbHelper = new JaxbHelper();
        StringWriter stringWriter = new StringWriter();
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper(userName);
            ProductionService productionService = calvalusHelper.getProductionService();
            Production production = productionService.getProduction(productionId);

//            String userName = production.getProductionRequest().getUserName();
//            productionService.updateStatuses(userName);
            ProcessStatus processingStatus = production.getProcessingStatus();

            if (production.getStagingStatus().getState() == ProcessState.COMPLETED) {
                List<String> productResultUrls = calvalusHelper.getProductResultUrls(production);
                ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
                ExecuteResponse executeResponse = executeSuccessfulResponse.getExecuteResponse(productResultUrls);
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            } else if (production.getProcessingStatus().getState() == ProcessState.COMPLETED) {
                calvalusHelper.stageProduction(productionService, production);
                List<String> productResultUrls = calvalusHelper.getProductResultUrls(production);
                ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
                ExecuteResponse executeResponse = executeSuccessfulResponse.getExecuteResponse(productResultUrls);
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            } else if (production.getProcessingStatus().getState().isDone()) {
                ExecuteFailedResponse executeFailedResponse = new ExecuteFailedResponse();
                ExecuteResponse executeResponse = executeFailedResponse.getExecuteResponse(production.getProcessingStatus().getMessage());
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            } else {
                ExecuteStartedResponse executeStartedResponse = new ExecuteStartedResponse();
                ExecuteResponse executeResponse = executeStartedResponse.getExecuteResponse(processingStatus.getState().toString(), 100 * processingStatus.getProgress());
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            }

        } catch (ProductionException | IOException | InterruptedException | DatatypeConfigurationException | JAXBException e) {
            e.printStackTrace();
            StringWriter exceptionStringWriter = new StringWriter();
            ExecuteFailedResponse executeFailedResponse = new ExecuteFailedResponse();
            try {
                ExecuteResponse executeResponse = executeFailedResponse.getExecuteResponse(e.getMessage());
                jaxbHelper.marshal(executeResponse, exceptionStringWriter);
            } catch (JAXBException | DatatypeConfigurationException exception) {
                exception.printStackTrace();
            }
            return exceptionStringWriter.toString();
        }
    }
}
