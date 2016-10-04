package com.bc.calvalus.wps;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.wpsoperations.CalvalusDescribeProcessOperation;
import com.bc.calvalus.wps.wpsoperations.CalvalusExecuteOperation;
import com.bc.calvalus.wps.wpsoperations.CalvalusGetCapabilitiesOperation;
import com.bc.calvalus.wps.wpsoperations.CalvalusGetStatusOperation;
import com.bc.ceres.binding.BindingException;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServiceInstance;
import com.bc.wps.api.exceptions.WpsServiceException;
import com.bc.wps.api.schema.Capabilities;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ProcessDescriptionType;
import com.bc.wps.utilities.WpsLogger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class CalvalusWpsProvider implements WpsServiceInstance {

    private Logger logger = WpsLogger.getLogger();

    @Override
    public Capabilities getCapabilities(WpsRequestContext wpsRequestContext) throws WpsServiceException {
        try {
            CalvalusGetCapabilitiesOperation getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(wpsRequestContext);
            return getCapabilitiesOperation.getCapabilities();
        } catch (WpsProcessorNotFoundException | IOException | URISyntaxException | BindingException exception) {
            logger.log(Level.SEVERE, "Unable to perform GetCapabilities operation successfully", exception);
            throw new WpsServiceException(exception);
        }
    }

    @Override
    public List<ProcessDescriptionType> describeProcess(WpsRequestContext wpsRequestContext, String processId) throws WpsServiceException {
        try {
            CalvalusDescribeProcessOperation describeProcessOperation = new CalvalusDescribeProcessOperation(wpsRequestContext);
            return describeProcessOperation.getProcesses(processId);
        } catch (WpsProcessorNotFoundException | IOException exception) {
            logger.log(Level.SEVERE, "Unable to perform DescribeProcess operation successfully", exception);
            throw new WpsServiceException(exception);
        }
    }

    @Override
    public ExecuteResponse doExecute(WpsRequestContext wpsRequestContext, Execute execute) throws WpsServiceException {
        try {
            CalvalusExecuteOperation executeOperation = new CalvalusExecuteOperation(wpsRequestContext);
            return executeOperation.execute(execute);
        } catch (IOException | WpsStagingException | ProductionException | WpsProductionException |
                    WpsResultProductException | InvalidProcessorIdException | JAXBException exception) {
            logger.log(Level.SEVERE, "Unable to perform Execute operation successfully", exception);
            throw new WpsServiceException(exception);
        }
    }

    @Override
    public ExecuteResponse getStatus(WpsRequestContext wpsRequestContext, String jobId) throws WpsServiceException {
        try {
            CalvalusGetStatusOperation getStatusOperation = new CalvalusGetStatusOperation(wpsRequestContext);
            return getStatusOperation.getStatus(jobId);
        } catch (IOException exception) {
            logger.log(Level.SEVERE, "Unable to perform GetStatus operation successfully", exception);
            throw new WpsServiceException(exception);
        }
    }

    @Override
    public void dispose() {
        Timer statusObserver = CalvalusProductionService.getStatusObserverSingleton();
        statusObserver.cancel();
        System.out.println("********************************************");
        System.out.println("****** Stopping Calvalus WPS intance  ******");
        System.out.println("********************************************");
    }
}
