package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProcessorExtractor;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ExceptionType;
import com.bc.calvalus.wpsrest.responses.GetCapabilitiesResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

/**
 * Created by hans on 21/08/2015.
 */
@Path("/GetCapabilities")
public class GetCapabilitiesService {

    @GET
    @Produces(MediaType.APPLICATION_XML)
    public String getCapabilities() {
        StringWriter writer = new StringWriter();
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper();

            List<Processor> processors = calvalusHelper.getProcessors();

            GetCapabilitiesResponse getCapabilitiesResponse = new GetCapabilitiesResponse();
            Capabilities capabilities = getCapabilitiesResponse.createGetCapabilitiesResponse(processors);

            JaxbHelper jaxbHelper = new JaxbHelper();
            jaxbHelper.marshal(capabilities, writer);
            return writer.toString();
        } catch (ProductionException | IOException | JAXBException exception) {
            exception.printStackTrace();
            return createExceptionResponse(writer, exception);
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

}
