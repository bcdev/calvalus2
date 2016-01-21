package com.bc.calvalus.wps;

import com.bc.calvalus.wps.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wps.exceptions.FailedRequestException;
import com.bc.calvalus.wps.exceptions.JobNotFoundException;
import com.bc.calvalus.wps.exceptions.ProcessesNotAvailableException;
import com.bc.calvalus.wps.wpsoperations.describeprocess.CalvalusDescribeProcessOperation;
import com.bc.calvalus.wps.wpsoperations.execute.CalvalusExecuteOperation;
import com.bc.calvalus.wps.wpsoperations.getcapabilities.CalvalusGetCapabilitiesOperation;
import com.bc.calvalus.wps.wpsoperations.getstatus.CalvalusGetStatusOperation;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServiceException;
import com.bc.wps.api.WpsServiceInstance;
import com.bc.wps.api.schema.Capabilities;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ProcessDescriptionType;
import com.bc.wps.utilities.WpsLogger;

import javax.xml.bind.JAXBException;
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
        CalvalusGetCapabilitiesOperation getCapabilitiesOperation = new CalvalusGetCapabilitiesOperation(wpsRequestContext);
        try {
            return getCapabilitiesOperation.getCapabilities();
        } catch (ProcessesNotAvailableException | JAXBException exception) {
            logger.log(Level.SEVERE, "Unable to perform GetCapabilities operation successfully", exception);
            throw new WpsServiceException("Unable to perform GetCapabilities operation successfully", exception);
        }
    }

    @Override
    public List<ProcessDescriptionType> describeProcess(WpsRequestContext wpsRequestContext, String processId) throws WpsServiceException {
        CalvalusDescribeProcessOperation describeProcessOperation = new CalvalusDescribeProcessOperation(wpsRequestContext);
        try {
            return describeProcessOperation.getProcesses(processId);
        } catch (ProcessesNotAvailableException exception) {
            logger.log(Level.SEVERE, "Unable to perform DescribeProcess operation successfully", exception);
            throw new WpsServiceException("Unable to perform DescribeProcess operation successfully", exception);
        }
    }

    @Override
    public ExecuteResponse doExecute(WpsRequestContext wpsRequestContext, Execute execute) throws WpsServiceException {
        CalvalusExecuteOperation executeOperation = new CalvalusExecuteOperation(wpsRequestContext);
        try {
            return executeOperation.execute(execute);
        } catch (FailedRequestException exception) {
            logger.log(Level.SEVERE, "Unable to perform Execute operation successfully", exception);
            throw new WpsServiceException("Unable to perform Execute operation successfully", exception);
        }
    }

    @Override
    public ExecuteResponse getStatus(WpsRequestContext wpsRequestContext, String jobId) throws WpsServiceException {
        CalvalusGetStatusOperation getStatusOperation = new CalvalusGetStatusOperation(wpsRequestContext);
        try {
            return getStatusOperation.getStatus(jobId);
        } catch (JobNotFoundException exception) {
            logger.log(Level.SEVERE, "Unable to perform GetStatus operation successfully", exception);
            throw new WpsServiceException("Unable to perform GetStatus operation successfully", exception);
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
