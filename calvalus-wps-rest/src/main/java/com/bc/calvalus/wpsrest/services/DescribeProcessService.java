package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.exception.ProcessorNotAvailableException;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.responses.CalvalusDescribeProcessResponse;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.IWpsProcess;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles all DescribeProcess requests.
 *
 * @author hans
 */
public class DescribeProcessService {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public String describeProcess(WpsMetadata wpsMetadata, String processorId) {
        StringWriter writer = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();

        String[] processorIdArray = processorId.split(",");
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper(wpsMetadata.getServletRequestWrapper());

            ProcessDescriptions processDescriptions;
            if (processorId.equalsIgnoreCase("all")) {
                CalvalusDescribeProcessResponse describeProcessResponse = new CalvalusDescribeProcessResponse(wpsMetadata);
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(calvalusHelper.getProcessors());
            } else if (processorIdArray.length > 1) {
                List<IWpsProcess> processors = new ArrayList<>();
                for (String singleProcessorId : processorIdArray) {
                    ProcessorNameParser parser = new ProcessorNameParser(singleProcessorId);
                    Processor processor = calvalusHelper.getProcessor(parser);
                    if (processor == null) {
                        throw new ProcessorNotAvailableException(singleProcessorId);
                    }
                    processors.add(processor);
                }
                CalvalusDescribeProcessResponse describeProcessResponse = new CalvalusDescribeProcessResponse(wpsMetadata);
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(processors);
            } else {
                ProcessorNameParser parser = new ProcessorNameParser(processorId);
                Processor processor = calvalusHelper.getProcessor(parser);
                if (processor == null) {
                    throw new ProcessorNotAvailableException(processorId);
                }
                CalvalusDescribeProcessResponse describeProcessResponse = new CalvalusDescribeProcessResponse(wpsMetadata);
                processDescriptions = describeProcessResponse.getSingleDescribeProcessResponse(processor);
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
