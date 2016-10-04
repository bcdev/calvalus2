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
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import com.bc.wps.api.exceptions.MissingParameterValueException;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.utilities.PropertiesWrapper;
import com.bc.wps.utilities.WpsLogger;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.OperatorException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class LocalFacade extends ProcessFacade {

    private static final String CATALINA_BASE = System.getProperty("catalina.base");

    private Logger logger = WpsLogger.getLogger();

    private final ProcessorExtractor processorExtractor;
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
        this.wpsRequestContext = wpsRequestContext;
    }

    @Override
    public LocalProductionStatus orderProductionAsynchronous(Execute executeRequest) throws WpsProductionException {
        try {
            ProcessBuilder processBuilder = getProcessBuilder(executeRequest);
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] starting asynchronous process...");
            LocalProductionStatus status = new LocalProductionStatus(processBuilder.getJobId(),
                                                                     ProcessState.SCHEDULED,
                                                                     0,
                                                                     "The request has been queued.",
                                                                     null);
            GpfProductionService.getProductionStatusMap().put(processBuilder.getJobId(), status);
            GpfTask gpfTask = new GpfTask(this,
                                          processBuilder.getServerContext().getHostAddress(),
                                          processBuilder.getServerContext().getPort(),
                                          processBuilder);
            GpfProductionService.getWorker().submit(gpfTask);
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] job has been queued...");
            return status;
        } catch (InvalidParameterValueException | JAXBException | MissingParameterValueException | IOException exception) {
            throw new WpsProductionException(exception);
        }
    }

    @Override
    public LocalProductionStatus orderProductionSynchronous(Execute executeRequest) throws WpsProductionException {
        LocalProductionStatus status;
        ProcessBuilder processBuilder = ProcessBuilder.create();
        try {
            processBuilder = getProcessBuilder(executeRequest);
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] starting synchronous process...");
            Product subset = GPF.createProduct("Subset", processBuilder.getParameters(), processBuilder.getSourceProduct());
            String outputFormat = (String) processBuilder.getParameters().get("outputFormat");
            GPF.writeProduct(subset,
                             new File(processBuilder.getTargetDirPath().toFile(), processBuilder.getSourceProduct().getName() + ".nc"),
                             outputFormat,
                             false,
                             ProgressMonitor.NULL);
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] constructing result URLs...");
            List<String> resultUrls = getProductResultUrls(processBuilder.getJobId());
            ProcessorNameConverter nameConverter = new ProcessorNameConverter(processBuilder.getProcessId());
            ProcessorExtractor processorExtractor = new ProcessorExtractor();
            LocalMetadata localMetadata = new LocalMetadata();
            localMetadata.generateProductMetadata(processBuilder.getTargetDirPath().toFile(),
                                                  processBuilder.getJobId(),
                                                  processBuilder.getParameters(),
                                                  processorExtractor.getProcessor(nameConverter),
                                                  processBuilder.getServerContext().getHostAddress(),
                                                  processBuilder.getServerContext().getPort());
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] job has been completed, creating successful response...");
            status = new LocalProductionStatus(processBuilder.getJobId(),
                                               ProcessState.COMPLETED,
                                               100,
                                               "The request has been processed successfully.",
                                               resultUrls);
            status.setStopDate(new Date());
            return status;
        } catch (WpsResultProductException | OperatorException | ProductionException exception) {
            status = new LocalProductionStatus(processBuilder.getJobId(),
                                               ProcessState.ERROR,
                                               0,
                                               "Processing failed : " + exception.getMessage(),
                                               null);
            status.setStopDate(new Date());
            return status;
        } catch (ProductMetadataException | IOException | URISyntaxException |
                    InvalidProcessorIdException | BindingException | InvalidParameterValueException |
                    JAXBException | MissingParameterValueException exception) {
            String jobId = processBuilder.getJobId();
            status = new LocalProductionStatus(jobId,
                                               ProcessState.ERROR,
                                               100,
                                               "Creating product metadata failed : " + exception.getMessage(),
                                               null);
            status.setStopDate(new Date());
            GpfProductionService.getProductionStatusMap().put(jobId, status);
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

    private Product getSourceProduct(Map<String, String> inputParameters) throws IOException {
        final Product sourceProduct;
        Path dir = Paths.get(CATALINA_BASE + PropertiesWrapper.get("wps.application.path"), PropertiesWrapper.get("utep.input.directory"));
        List<File> files = new ArrayList<>();
        DirectoryStream<Path> stream = Files.newDirectoryStream(dir, inputParameters.get("sourceProduct"));
        for (Path entry : stream) {
            files.add(entry.toFile());
        }

        String sourceProductPath;
        if (files.size() != 0) {
            sourceProductPath = files.get(0).getAbsolutePath();
        } else {
            throw new FileNotFoundException("The source product '" + inputParameters.get("sourceProduct") + "' cannot be found");
        }

        sourceProduct = ProductIO.readProduct(sourceProductPath);
        return sourceProduct;
    }

    private Path getTargetDirectoryPath(String jobId) throws IOException {
        Path targetDirectoryPath = Paths.get(CATALINA_BASE + PropertiesWrapper.get("wps.application.path"),
                                             PropertiesWrapper.get("utep.output.directory"), getUserName(), jobId);
        Files.createDirectories(targetDirectoryPath);
        return targetDirectoryPath;
    }

    private ProcessBuilder getProcessBuilder(Execute executeRequest)
                throws JAXBException, MissingParameterValueException, InvalidParameterValueException, IOException {
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);
        Map<String, String> inputParameters = requestExtractor.getInputParametersMapRaw();
        final Product sourceProduct = getSourceProduct(inputParameters);
        String jobId = GpfProductionService.createJobId(getUserName());
        String processId = executeRequest.getIdentifier().getValue();
        Path targetDirPath = getTargetDirectoryPath(jobId);
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("productionName", inputParameters.get("productionName"));
        parameters.put("geoRegion", inputParameters.get("regionWKT"));
        parameters.put("outputFormat", inputParameters.get("outputFormat"));
        parameters.put("productionType", inputParameters.get("productionType"));
        parameters.put("sourceProduct", inputParameters.get("sourceProduct"));
        parameters.put("copyMetadata", inputParameters.get("copyMetadata"));
        parameters.put("targetDir", targetDirPath.toString());
        return ProcessBuilder.create()
                    .withJobId(jobId)
                    .withProcessId(processId)
                    .withParameters(parameters)
                    .withSourceProduct(sourceProduct)
                    .withTargetDirPath(targetDirPath)
                    .withServerContext(wpsRequestContext.getServerContext())
                    .withExecuteRequest(executeRequest);
    }
}
