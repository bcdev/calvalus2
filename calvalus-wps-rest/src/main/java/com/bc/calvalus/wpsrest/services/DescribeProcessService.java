package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.WpsException;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ExceptionType;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.responses.DescribeProcessResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Created by hans on 21/08/2015.
 */
@Path("/DescribeProcess")
public class DescribeProcessService {

    @GET
    @Path("{processorId}")
    @Produces(MediaType.APPLICATION_XML)
    public String describeProcess(@PathParam("processorId") String processorId) {
        StringWriter writer = new StringWriter();
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper();
            ProcessorNameParser parser = new ProcessorNameParser(processorId);
            Processor testProcessor = calvalusHelper.getProcessor(parser);

            DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
            ProcessDescriptions processDescriptions = describeProcessResponse.getDescribeProcessResponse(testProcessor, calvalusHelper.getProductSets());

            JaxbHelper jaxbHelper = new JaxbHelper();
            jaxbHelper.marshal(processDescriptions, writer);
            return writer.toString();
        } catch (ProductionException | WpsException | IOException | JAXBException exception) {
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
