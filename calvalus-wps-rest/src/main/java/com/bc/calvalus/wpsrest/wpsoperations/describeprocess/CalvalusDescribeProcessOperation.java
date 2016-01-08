package com.bc.calvalus.wpsrest.wpsoperations.describeprocess;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.CalvalusProcessor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.ProcessorNotAvailableException;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.responses.AbstractDescribeProcessResponseConverter;
import com.bc.calvalus.wpsrest.responses.CalvalusDescribeProcessResponseConverter;
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

    public CalvalusDescribeProcessOperation(WpsMetadata wpsMetadata, String processorId) {
        super(wpsMetadata, processorId);
    }

    @Override
    public ProcessDescriptions getProcessDescriptions(WpsMetadata wpsMetadata, String processorId) {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(wpsMetadata.getServletRequestWrapper());

            String[] processorIdArray = processorId.split(",");

            ProcessDescriptions processDescriptions;
            if (processorId.equalsIgnoreCase("all")) {
                AbstractDescribeProcessResponseConverter describeProcessResponse = new CalvalusDescribeProcessResponseConverter(wpsMetadata);
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(calvalusFacade.getProcessors());
            } else if (processorIdArray.length > 1) {
                List<IWpsProcess> processors = new ArrayList<>();
                for (String singleProcessorId : processorIdArray) {
                    ProcessorNameParser parser = new ProcessorNameParser(singleProcessorId);
                    CalvalusProcessor calvalusProcessor = calvalusFacade.getProcessor(parser);
                    if (calvalusProcessor == null) {
                        throw new ProcessorNotAvailableException(singleProcessorId);
                    }
                    processors.add(calvalusProcessor);
                }
                AbstractDescribeProcessResponseConverter describeProcessResponse = new CalvalusDescribeProcessResponseConverter(wpsMetadata);
                processDescriptions = describeProcessResponse.getMultipleDescribeProcessResponse(processors);
            } else {
                ProcessorNameParser parser = new ProcessorNameParser(processorId);
                CalvalusProcessor calvalusProcessor = calvalusFacade.getProcessor(parser);
                if (calvalusProcessor == null) {
                    throw new ProcessorNotAvailableException(processorId);
                }
                AbstractDescribeProcessResponseConverter describeProcessResponse = new CalvalusDescribeProcessResponseConverter(wpsMetadata);
                processDescriptions = describeProcessResponse.getSingleDescribeProcessResponse(calvalusProcessor);
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
