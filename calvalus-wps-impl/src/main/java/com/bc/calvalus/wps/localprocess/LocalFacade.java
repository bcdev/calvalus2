package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.ceres.binding.BindingException;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import com.bc.wps.api.exceptions.MissingParameterValueException;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.utilities.WpsLogger;
import org.esa.snap.core.gpf.OperatorException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class LocalFacade extends ProcessFacade {

    private Logger logger = WpsLogger.getLogger();

    private final ProcessorExtractor processorExtractor;
    private final LocalProduction localProduction;
    private final LocalStaging localStaging;
    private final String hostName;
    private final int portNumber;
    private final WpsRequestContext wpsRequestContext;

    public LocalFacade(WpsRequestContext wpsRequestContext) throws IOException {
        super(wpsRequestContext);
        WpsServerContext serverContext = wpsRequestContext.getServerContext();
        this.hostName = serverContext.getHostAddress();
        this.portNumber = serverContext.getPort();
        this.processorExtractor = new ProcessorExtractor();
        this.localStaging = new LocalStaging();
        this.localProduction = new LocalProduction();
        this.wpsRequestContext = wpsRequestContext;
    }

    @Override
    public LocalProductionStatus orderProductionAsynchronous(Execute executeRequest) throws WpsProductionException {
        ProcessBuilder processBuilder = ProcessBuilder.create();
        try {
            processBuilder = localProduction.getProcessBuilder(executeRequest, userName, wpsRequestContext.getServerContext());
            return localProduction.orderProductionAsynchronous(this, processBuilder);
        } catch (InvalidParameterValueException | JAXBException | MissingParameterValueException | IOException exception) {
            String jobId = processBuilder.getJobId();
            LocalProductionStatus status = getFailedStatus("Processing failed : " + exception.getMessage(), processBuilder);
            logger.log(Level.SEVERE, "[" + jobId + "] Processing failed : ", exception);
            updateProductionStatusMap(status, processBuilder);
            return status;
        }
    }

    @Override
    public LocalProductionStatus orderProductionSynchronous(Execute executeRequest) throws WpsProductionException {
        LocalProductionStatus status;
        ProcessBuilder processBuilder = ProcessBuilder.create();
        try {
            processBuilder = localProduction.getProcessBuilder(executeRequest, userName, wpsRequestContext.getServerContext());
            String jobId = processBuilder.getJobId();
            localProduction.orderProductionSynchronous(processBuilder);
            status = getSuccessfulStatus(processBuilder, getProductResultUrls(jobId));
            updateProductionStatusMap(status, processBuilder);
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] Generating product metadata...");
            generateProductMetadata(jobId);
            return status;
        } catch (WpsResultProductException | OperatorException | ProductionException | InvalidParameterValueException |
                    JAXBException | MissingParameterValueException exception) {
            String jobId = processBuilder.getJobId();
            status = getFailedStatus("Processing failed : " + exception.getMessage(), processBuilder);
            updateProductionStatusMap(status, processBuilder);
            logger.log(Level.SEVERE, "[" + jobId + "] Processing failed : ", exception);
            return status;
        } catch (ProductMetadataException | IOException | URISyntaxException |
                    BindingException | InvalidProcessorIdException exception) {
            String jobId = processBuilder.getJobId();
            status = getFailedStatus("Creating product metadata failed : " + exception.getMessage(), processBuilder);
            updateProductionStatusMap(status, processBuilder);
            logger.log(Level.SEVERE, "[" + jobId + "] Creating product metadata failed...", exception);
            return status;
        }
    }

    @Override
    public List<String> getProductResultUrls(String jobId) throws WpsResultProductException {
        return localStaging.getProductUrls(jobId, userName, hostName, portNumber);
    }

    @Override
    public void stageProduction(String jobId) throws WpsStagingException {

    }

    @Override
    public void observeStagingStatus(String jobId) throws WpsStagingException {

    }

    @Override
    public void generateProductMetadata(String jobId) throws ProductMetadataException {
        LocalJob job = GpfProductionService.getProductionStatusMap().get(jobId);
        if (job == null) {
            throw new ProductMetadataException("Unable to create metadata for jobId '" + jobId + "'");
        }
        try {
            String processId = (String) job.getParameters().get("processId");
            ProcessorNameConverter processorNameConverter = new ProcessorNameConverter(processId);
            WpsProcess processor = getProcessor(processorNameConverter);
            String targetDirPath = (String) job.getParameters().get("targetDir");
            File targetDir = new File(targetDirPath);
            localStaging.generateProductMetadata(targetDir, job.getJobid(), job.getParameters(), processor, hostName, portNumber);
        } catch (InvalidProcessorIdException | ProductionException | WpsProcessorNotFoundException | FileNotFoundException exception) {
            throw new ProductMetadataException(exception);
        }
    }

    @Override
    public List<WpsProcess> getProcessors() throws WpsProcessorNotFoundException {
        try {
            return processorExtractor.getProcessors();
        } catch (BindingException | IOException | URISyntaxException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
    }

    @Override
    public WpsProcess getProcessor(ProcessorNameConverter parser) throws WpsProcessorNotFoundException {
        try {
            return processorExtractor.getProcessor(parser);
        } catch (BindingException | IOException | URISyntaxException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
    }

    private void updateProductionStatusMap(LocalProductionStatus status, ProcessBuilder processBuilder) {
        String jobId = processBuilder.getJobId();
        LocalJob job = new LocalJob(jobId, processBuilder.getParameters(), status);
        GpfProductionService.getProductionStatusMap().put(jobId, job);
    }

    private LocalProductionStatus getFailedStatus(String errorMessage, ProcessBuilder processBuilder) {
        LocalProductionStatus status;
        status = new LocalProductionStatus(processBuilder.getJobId(),
                                           ProcessState.ERROR,
                                           0,
                                           errorMessage,
                                           null);
        status.setStopDate(new Date());
        return status;
    }

    private LocalProductionStatus getSuccessfulStatus(ProcessBuilder processBuilder, List<String> resultUrls) {
        LocalProductionStatus status;
        status = new LocalProductionStatus(processBuilder.getJobId(),
                                           ProcessState.COMPLETED,
                                           100,
                                           "The request has been processed successfully.",
                                           resultUrls);
        status.setStopDate(new Date());
        return status;
    }
}
