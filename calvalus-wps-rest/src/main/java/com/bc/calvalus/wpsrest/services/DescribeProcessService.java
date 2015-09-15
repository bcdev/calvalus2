package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.WpsException;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.responses.DescribeProcessResponse;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hans on 21/08/2015.
 */
@Path("/DescribeProcess")
public class DescribeProcessService {

    @GET
    @Path("{processorId}")
    @Produces(MediaType.APPLICATION_XML)
    public String describeProcess(String userName, @PathParam("processorId") String processorId) {
        StringWriter writer = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();

        String[] processorIdArray = processorId.split(",");
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper(userName);

            ProcessDescriptions processDescriptions;
            if (processorId.equalsIgnoreCase("all")) {
                DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(calvalusHelper.getProcessors(), calvalusHelper.getProductSets());
            } else if (processorIdArray.length > 1) {
                List<Processor> processors = new ArrayList<>();
                for (String singleProcessorId : processorIdArray) {
                    ProcessorNameParser parser = new ProcessorNameParser(singleProcessorId);
                    Processor processor = calvalusHelper.getProcessor(parser);
                    processors.add(processor);
                }
                DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(processors, calvalusHelper.getProductSets());
            } else {
                ProcessorNameParser parser = new ProcessorNameParser(processorId);
                Processor processor = calvalusHelper.getProcessor(parser);
                DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
                processDescriptions = describeProcessResponse.getSingleDescribeProcessResponse(processor, calvalusHelper.getProductSets());
            }

            jaxbHelper.marshal(processDescriptions, writer);
            return writer.toString();
        } catch (ProductionException | IOException | JAXBException exception) {
            exception.printStackTrace();
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            ExceptionReport exceptionReport = exceptionResponse.getGeneralExceptionResponse(exception);
            try {
                jaxbHelper.marshal(exceptionReport, writer);
            } catch (JAXBException e) {
                e.printStackTrace();
            }
            return writer.toString();
        } catch (WpsException exception) {
            exception.printStackTrace();
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            ExceptionReport exceptionReport = exceptionResponse.getInvalidParameterExceptionResponse(exception, "Identifier");
            try {
                jaxbHelper.marshal(exceptionReport, writer);
            } catch (JAXBException e) {
                e.printStackTrace();
            }
            return writer.toString();
        }
    }
}
