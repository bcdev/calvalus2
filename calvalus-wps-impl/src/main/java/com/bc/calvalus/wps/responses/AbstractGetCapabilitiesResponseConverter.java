package com.bc.calvalus.wps.responses;



import com.bc.wps.api.schema.Capabilities;
import com.bc.wps.api.schema.Languages;
import com.bc.wps.api.schema.OperationsMetadata;
import com.bc.wps.api.schema.ProcessOfferings;
import com.bc.wps.api.schema.ServiceIdentification;
import com.bc.wps.api.schema.ServiceProvider;
import com.bc.wps.api.utils.CapabilitiesBuilder;

import java.util.List;

/**
 * @author hans
 */
public abstract class AbstractGetCapabilitiesResponseConverter {

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

    protected abstract OperationsMetadata getOperationsMetadata();

    public abstract ServiceProvider getServiceProvider();

    public abstract ProcessOfferings getProcessOfferings(List<IWpsProcess> processList);

    public abstract ServiceIdentification getServiceIdentification();

    public abstract Languages getLanguages();
}
