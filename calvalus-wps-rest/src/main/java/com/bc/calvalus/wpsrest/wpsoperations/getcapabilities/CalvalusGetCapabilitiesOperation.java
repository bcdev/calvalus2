package com.bc.calvalus.wpsrest.wpsoperations.getcapabilities;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.ProcessesNotAvailableException;
import com.bc.calvalus.wpsrest.responses.IWpsProcess;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusGetCapabilitiesOperation extends AbstractGetCapabilitiesOperation {

    public CalvalusGetCapabilitiesOperation(WpsMetadata wpsMetadata) {
        super(wpsMetadata);
    }

    @Override
    public Logger getLogger() {
        return CalvalusLogger.getLogger();
    }

    @Override
    public List<IWpsProcess> getProcesses() throws ProcessesNotAvailableException {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(wpsMetadata.getServletRequestWrapper());
            return calvalusFacade.getProcessors();
        } catch (IOException | ProductionException exception) {
            throw new ProcessesNotAvailableException("Unable to retrieve available processors", exception);
        }
    }
}
