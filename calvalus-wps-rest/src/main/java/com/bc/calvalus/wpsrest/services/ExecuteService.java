package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.WpsException;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProcessorExtractor;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProduction;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusStaging;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ExceptionType;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteAcceptedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteSuccessfulResponse;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hans on 21/08/2015.
 */
@Path("/Execute")
public class ExecuteService {

    private static final String WEBAPPS_ROOT = "/webapps/ROOT/";
    private static final String PORT_NUMBER = "9080";

    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    public String executeWithoutProcessor(Execute execute) {
        StringBuilder sb = new StringBuilder();
        sb.append("no processor specified");
        return sb.toString();
    }

    @POST
    @Path("{processorId}")
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    public String execute(Execute execute, @PathParam("processorId") String processorId) {
        StringWriter stringWriter = new StringWriter();
        try {
            ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(execute);

            CalvalusConfig calvalusConfig = new CalvalusConfig();
            CalvalusProduction calvalusProduction = new CalvalusProduction();
            CalvalusStaging calvalusStaging = new CalvalusStaging();

            ProductionService productionService = CalvalusProductionService.getInstance(calvalusConfig);
            CalvalusProcessorExtractor calvalusProcessorExtractor = new CalvalusProcessorExtractor(productionService);
            ProcessorNameParser parser = new ProcessorNameParser(processorId);
            Processor processor = calvalusProcessorExtractor.getProcessor(parser);
            CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, processor);

            ProductionRequest request = new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                                              getSystemUserName(),
                                                              calvalusDataInputs.getInputMapFormatted());


            Production production = calvalusProduction.orderProduction(productionService, request);
//            calvalusStaging.stageProduction(productionService, production);

//            if (productionService != null) {
//                productionService.close();
//            }

            ExecuteAcceptedResponse executeAcceptedResponse = new ExecuteAcceptedResponse();
            ExecuteResponse executeResponse = executeAcceptedResponse.getExecuteResponse(production.getId());
            JaxbHelper jaxbHelper = new JaxbHelper();
            jaxbHelper.marshal(executeResponse, stringWriter);
            return stringWriter.toString();
        } catch (InterruptedException | IOException | JAXBException | DatatypeConfigurationException | ProductionException | WpsException exception) {
            exception.printStackTrace();
            return createExceptionResponse(stringWriter, exception);
        }
    }

    private String createExceptionResponse(Writer writer, Exception exception) {
        ExceptionReport exceptionReport = new ExceptionReport();
        ExceptionType exceptionResponse = new ExceptionType();
        exceptionResponse.getExceptionText().add(exception.getMessage());
        exceptionResponse.setExceptionCode("exceptionCode");
        exceptionResponse.setLocator("locator");

        exceptionReport.getException().add(exceptionResponse);
        exceptionReport.setLang("Lang");
        exceptionReport.setVersion("version");
        JaxbHelper jaxbHelper = new JaxbHelper();
        try {
            jaxbHelper.marshal(exceptionReport, writer);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return writer.toString();
    }

    private String getSystemUserName() {
        return System.getProperty("user.name", "anonymous").toLowerCase();
    }

}
