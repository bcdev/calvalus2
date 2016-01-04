package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptionType;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import java.util.List;

/**
 * @author hans
 */
public abstract class AbstractDescribeProcessResponse {

    private WpsMetadata wpsMetadata;

    public AbstractDescribeProcessResponse(WpsMetadata wpsMetadata) {
        this.wpsMetadata = wpsMetadata;
    }

    public ProcessDescriptions getMultipleDescribeProcessResponse(List<IWpsProcess> processes) {
        ProcessDescriptions processDescriptions = createBasicProcessDescriptions();
        for (IWpsProcess process : processes) {
            ProcessDescriptionType processDescription = getSingleProcessDescription(process, wpsMetadata);
            processDescriptions.getProcessDescription().add(processDescription);
        }
        return processDescriptions;
    }

    public ProcessDescriptions getSingleDescribeProcessResponse(Processor processor) {
        ProcessDescriptions processDescriptions = createBasicProcessDescriptions();
        ProcessDescriptionType processDescription = getSingleProcessDescription(processor, wpsMetadata);
        processDescriptions.getProcessDescription().add(processDescription);
        return processDescriptions;
    }

    public abstract ProcessDescriptions createBasicProcessDescriptions();

    public abstract ProcessDescriptionType getSingleProcessDescription(IWpsProcess process, WpsMetadata wpsMetadata);

}
