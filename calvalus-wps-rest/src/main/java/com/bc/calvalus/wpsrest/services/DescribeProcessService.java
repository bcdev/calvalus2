package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.exception.ProcessorNotAvailableException;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.responses.DescribeProcessResponse;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.WpsProcess;

import javax.ws.rs.PathParam;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles all DescribeProcess requests.
 * <p/>
 * Created by hans on 21/08/2015.
 */
public class DescribeProcessService {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public String describeProcess(ServletRequestWrapper servletRequestWrapper, @PathParam("processorId") String processorId) {
        StringWriter writer = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();

        String[] processorIdArray = processorId.split(",");
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper(servletRequestWrapper);

            ProcessDescriptions processDescriptions;
            if (processorId.equalsIgnoreCase("all")) {
                DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(calvalusHelper.getProcessors(), calvalusHelper.getProductSets());
            } else if (processorIdArray.length > 1) {
                List<WpsProcess> processors = new ArrayList<>();
                for (String singleProcessorId : processorIdArray) {
                    ProcessorNameParser parser = new ProcessorNameParser(singleProcessorId);
                    Processor processor = calvalusHelper.getProcessor(parser);
                    if (processor == null) {
                        throw new ProcessorNotAvailableException(singleProcessorId);
                    }
                    processors.add(processor);
                }
                DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(processors, calvalusHelper.getProductSets());
            } else {
                ProcessorNameParser parser = new ProcessorNameParser(processorId);
                Processor processor = calvalusHelper.getProcessor(parser);
                if (processor == null) {
                    throw new ProcessorNotAvailableException(processorId);
                }
                DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
                processDescriptions = describeProcessResponse.getSingleDescribeProcessResponse(processor, calvalusHelper.getProductSets());
            }

            jaxbHelper.marshal(processDescriptions, writer);
            return writer.toString();
        } catch (ProductionException | IOException | JAXBException | ProcessorNotAvailableException exception) {
            LOG.log(Level.SEVERE, "An error occurred when trying to construct a DescribeProcess response.", exception);
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            ExceptionReport exceptionReport = exceptionResponse.getGeneralExceptionResponse(exception);
            try {
                jaxbHelper.marshal(exceptionReport, writer);
            } catch (JAXBException jaxbException) {
                LOG.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
            }
            return writer.toString();
        }
    }
}
