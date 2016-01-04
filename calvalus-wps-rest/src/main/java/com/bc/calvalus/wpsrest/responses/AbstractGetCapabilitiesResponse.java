package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.Languages;
import com.bc.calvalus.wpsrest.jaxb.OperationsMetadata;
import com.bc.calvalus.wpsrest.jaxb.ProcessOfferings;
import com.bc.calvalus.wpsrest.jaxb.ServiceIdentification;
import com.bc.calvalus.wpsrest.jaxb.ServiceProvider;

import java.util.List;

/**
 * @author hans
 */
public abstract class AbstractGetCapabilitiesResponse {

    public Capabilities createGetCapabilitiesResponse(List<IWpsProcess> processList) {
        OperationsMetadata operationsMetadata = getOperationsMetadata();
        ServiceIdentification serviceIdentification = getServiceIdentification();
        ServiceProvider serviceProvider = getServiceProvider();
        ProcessOfferings processOfferings = getProcessOfferings(processList);
        Languages languages = getLanguages();

        return CapabilitiesBuilder.create()
                    .withOperationsMetadata(operationsMetadata)
                    .withServiceIdentification(serviceIdentification)
                    .withServiceProvider(serviceProvider)
                    .withProcessOfferings(processOfferings)
                    .withLanguages(languages)
                    .build();
    }

    public abstract OperationsMetadata getOperationsMetadata();

    public abstract ServiceProvider getServiceProvider();

    public abstract ProcessOfferings getProcessOfferings(List<IWpsProcess> processList);

    public abstract ServiceIdentification getServiceIdentification();

    public abstract Languages getLanguages();
}
