package com.bc.calvalus.wpsrest.wpsoperations.describeprocess;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.exception.ProcessorNotAvailableException;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.responses.AbstractDescribeProcessResponse;
import com.bc.calvalus.wpsrest.responses.CalvalusDescribeProcessResponse;
import com.bc.calvalus.wpsrest.responses.IWpsProcess;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusDescribeProcessOperation extends AbstractDescribeProcessOperation {

    @Override
    public ProcessDescriptions getProcessDescriptions(WpsMetadata wpsMetadata, String processorId) {
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper(wpsMetadata.getServletRequestWrapper());

            String[] processorIdArray = processorId.split(",");

            ProcessDescriptions processDescriptions;
            if (processorId.equalsIgnoreCase("all")) {
                AbstractDescribeProcessResponse describeProcessResponse = new CalvalusDescribeProcessResponse(wpsMetadata);
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
                AbstractDescribeProcessResponse describeProcessResponse = new CalvalusDescribeProcessResponse(wpsMetadata);
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(processors);
            } else {
                ProcessorNameParser parser = new ProcessorNameParser(processorId);
                Processor processor = calvalusHelper.getProcessor(parser);
                if (processor == null) {
                    throw new ProcessorNotAvailableException(processorId);
                }
                AbstractDescribeProcessResponse describeProcessResponse = new CalvalusDescribeProcessResponse(wpsMetadata);
                processDescriptions = describeProcessResponse.getSingleDescribeProcessResponse(processor);
            }
            return processDescriptions;
        } catch (IOException | ProductionException | ProcessorNotAvailableException exception) {
            throw new WpsException("Unable to create describe process response", exception);
        }
    }

    @Override
    public Logger getLogger() {
        return CalvalusLogger.getLogger();
    }
}
