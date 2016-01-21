package com.bc.calvalus.wps.wpsoperations;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.exceptions.ProcessesNotAvailableException;
import com.bc.calvalus.wps.responses.CalvalusGetCapabilitiesResponseConverter;
import com.bc.calvalus.wps.responses.IWpsProcess;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.Capabilities;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusGetCapabilitiesOperation {

    private WpsRequestContext context;

    public CalvalusGetCapabilitiesOperation(WpsRequestContext context) {
        this.context = context;
    }

    public Capabilities getCapabilities() throws ProcessesNotAvailableException, JAXBException {
        List<IWpsProcess> processes = getProcesses();
        CalvalusGetCapabilitiesResponseConverter getCapabilitiesResponse = new CalvalusGetCapabilitiesResponseConverter();

        return getCapabilitiesResponse.constructGetCapabilitiesResponse(processes);
    }

    private List<IWpsProcess> getProcesses() throws ProcessesNotAvailableException {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(context);
            return calvalusFacade.getProcessors();
        } catch (IOException | ProductionException exception) {
            throw new ProcessesNotAvailableException("Unable to retrieve available processors", exception);
        }
    }
}
