package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.CalvalusProcessor;
import com.bc.calvalus.wpsrest.exception.ProductSetsNotAvailableException;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptionType;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import java.util.List;

/**
 * @author hans
 */
public abstract class AbstractDescribeProcessResponseConverter {

    private WpsMetadata wpsMetadata;

    public AbstractDescribeProcessResponseConverter(WpsMetadata wpsMetadata) {
        this.wpsMetadata = wpsMetadata;
    }

    public ProcessDescriptions getMultipleDescribeProcessResponse(List<IWpsProcess> processes)
                throws ProductSetsNotAvailableException {
        ProcessDescriptions processDescriptions = createBasicProcessDescriptions();
        for (IWpsProcess process : processes) {
            ProcessDescriptionType processDescription = getSingleProcessDescription(process, wpsMetadata);
            processDescriptions.getProcessDescription().add(processDescription);
        }
        return processDescriptions;
    }

    public ProcessDescriptions getSingleDescribeProcessResponse(CalvalusProcessor calvalusProcessor)
                throws ProductSetsNotAvailableException {
        ProcessDescriptions processDescriptions = createBasicProcessDescriptions();
        ProcessDescriptionType processDescription = getSingleProcessDescription(calvalusProcessor, wpsMetadata);
        processDescriptions.getProcessDescription().add(processDescription);
        return processDescriptions;
    }

    public abstract ProcessDescriptions createBasicProcessDescriptions();

    public abstract ProcessDescriptionType getSingleProcessDescription(IWpsProcess process, WpsMetadata wpsMetadata) throws ProductSetsNotAvailableException;

}
