package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProduction;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusStaging;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteSuccessfulResponse;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
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

    @Context
    ServletContext context;

    public GetStatusService(@Context ServletContext context) {
        this.context = context;
    }

    @GET
    @Path("{productionId}")
    @Produces(MediaType.TEXT_PLAIN)
    public String execute(@PathParam("productionId") String productionId) {
        StringBuilder sb = new StringBuilder();

        CalvalusConfig calvalusConfig = new CalvalusConfig();
        CalvalusProduction calvalusProduction = new CalvalusProduction();
        CalvalusStaging calvalusStaging = new CalvalusStaging();
        try {
            ProductionService productionService = CalvalusProductionService.getInstance(calvalusConfig);
            Production production = productionService.getProduction(productionId);

            String userName = production.getProductionRequest().getUserName();
            productionService.updateStatuses(userName);
            ProcessStatus processingStatus = production.getProcessingStatus();
            sb.append(processingStatus.getState()).append(" ").append(processingStatus.getProgress()).append("\n");

//            calvalusProduction.observeProduction(productionService, production);

            if (production.getStagingStatus().getState() == ProcessState.COMPLETED) {
                List<String> productResultUrls = calvalusStaging.getProductResultUrls(calvalusConfig, production);
                ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
                ExecuteResponse executeResponse = executeSuccessfulResponse.getExecuteResponse(productResultUrls);
                JaxbHelper jaxbHelper = new JaxbHelper();
                StringWriter stringWriter = new StringWriter();
                jaxbHelper.marshal(executeResponse, stringWriter);
            } else if (production.getProcessingStatus().getState() == ProcessState.COMPLETED) {
                calvalusStaging.stageProduction(productionService, production);
                List<String> productResultUrls = calvalusStaging.getProductResultUrls(calvalusConfig, production);
                ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
                ExecuteResponse executeResponse = executeSuccessfulResponse.getExecuteResponse(productResultUrls);
                JaxbHelper jaxbHelper = new JaxbHelper();
                StringWriter stringWriter = new StringWriter();
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            }

            return sb.toString();
        } catch (ProductionException | IOException | InterruptedException | DatatypeConfigurationException | JAXBException e) {
            e.printStackTrace();
            sb.append("error");
            return sb.toString();
        }

    }

}
