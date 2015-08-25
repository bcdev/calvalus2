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
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteAcceptedResponse;

import javax.ws.rs.Consumes;
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

/**
 * Created by hans on 21/08/2015.
 */
@Path("/Execute")
public class ExecuteService {

    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    public String executeWithoutProcessor(Execute execute) {
        StringBuilder sb = new StringBuilder();
        sb.append("no processor specified\n");
        sb.append(new File("").getAbsolutePath());
        return sb.toString();
    }

    @POST
    @Path("{processorId}")
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    public String execute(Execute execute, @PathParam("processorId") String processorId) {
        StringWriter stringWriter = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();
        try {
            ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(execute);
            CalvalusHelper calvalusHelper = new CalvalusHelper();

            ProcessorNameParser parser = new ProcessorNameParser(processorId);
            Processor processor = calvalusHelper.getProcessor(parser);
            CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, processor);

            ProductionRequest request = new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                                              getSystemUserName(),
                                                              calvalusDataInputs.getInputMapFormatted());

            Production production = calvalusHelper.orderProduction(request);

            ExecuteAcceptedResponse executeAcceptedResponse = new ExecuteAcceptedResponse();
            ExecuteResponse executeResponse = executeAcceptedResponse.getExecuteResponse(production.getId());

            jaxbHelper.marshal(executeResponse, stringWriter);
            return stringWriter.toString();
        } catch (InterruptedException | IOException | JAXBException | DatatypeConfigurationException | ProductionException | WpsException exception) {
            exception.printStackTrace();
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            try {
                jaxbHelper.marshal(exceptionResponse.getExceptionResponse(exception), stringWriter);
            } catch (JAXBException e) {
                e.printStackTrace();
            }
            return stringWriter.toString();
        }
    }

    private String getSystemUserName() {
        return System.getProperty("user.name", "anonymous").toLowerCase();
    }
}
