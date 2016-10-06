package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.wps.utilities.WpsLogger;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.OperatorException;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class GpfTask implements Callable<Boolean> {


    private final String jobId;
    private final Map<String, Object> parameters;
    private final Product sourceProduct;
    private final File targetDir;
    private final String hostName;
    private final int portNumber;
    private final ProcessFacade localFacade;
    private final ProcessBuilder processBuilder;
    private Logger logger = WpsLogger.getLogger();

    public GpfTask(ProcessFacade localFacade, String hostName, int portNumber, ProcessBuilder processBuilder) {
        this.jobId = processBuilder.getJobId();
        this.parameters = processBuilder.getParameters();
        this.sourceProduct = processBuilder.getSourceProduct();
        this.targetDir = processBuilder.getTargetDirPath().toFile();
        this.hostName = hostName;
        this.portNumber = portNumber;
        this.localFacade = localFacade;
        this.processBuilder = processBuilder;
    }

    @Override
    public Boolean call() throws Exception {
        LocalJob job = GpfProductionService.getProductionStatusMap().get(jobId);
        LocalProductionStatus status = job.getStatus();
        status.setState(ProcessState.RUNNING);
        status.setProgress(10);
        try {
            logger.log(Level.INFO, "[" + jobId + "] starting subsetting operation...");
            Product subset = GPF.createProduct("Subset", parameters, sourceProduct);
            String outputFormat = (String) parameters.get("outputFormat");
            GPF.writeProduct(subset, new File(targetDir, sourceProduct.getName() + "-subset" + getFileExtension(outputFormat)), outputFormat,
                             false, ProgressMonitor.NULL);
            logger.log(Level.INFO, "[" + jobId + "] subsetting operation completed...");

            List<String> resultUrls = localFacade.getProductResultUrls(jobId);
            ProcessorExtractor processorExtractor = new ProcessorExtractor();
            ProcessorNameConverter nameConverter = new ProcessorNameConverter(processBuilder.getProcessId());
            LocalMetadata localMetadata = new LocalMetadata();
            localMetadata.generateProductMetadata(targetDir, jobId, parameters, processorExtractor.getProcessor(nameConverter), hostName, portNumber);
            status.setState(ProcessState.COMPLETED);
            status.setProgress(100);
            status.setResultUrls(resultUrls);
            status.setStopDate(new Date());
            job.updateStatus(status);
            GpfProductionService.getProductionStatusMap().put(jobId, job);
            return true;
        } catch (OperatorException exception) {
            status.setState(ProcessState.ERROR);
            status.setMessage("GPF process failed : " + exception.getMessage());
            status.setStopDate(new Date());
            job.updateStatus(status);
            GpfProductionService.getProductionStatusMap().put(jobId, job);
            logger.log(Level.SEVERE, "[" + jobId + "] GPF process failed...", exception);
            return false;
        } catch (ProductMetadataException exception) {
            status.setState(ProcessState.ERROR);
            status.setMessage("Creating product metadata failed : " + exception.getMessage());
            status.setStopDate(new Date());
            job.updateStatus(status);
            GpfProductionService.getProductionStatusMap().put(jobId, job);
            logger.log(Level.SEVERE, "[" + jobId + "] Creating product metadata failed...", exception);
            return false;
        } catch (Exception exception) {
            status.setState(ProcessState.ERROR);
            status.setMessage("Processing failed : " + exception.getMessage());
            status.setStopDate(new Date());
            job.updateStatus(status);
            GpfProductionService.getProductionStatusMap().put(jobId, job);
            logger.log(Level.SEVERE, "[" + jobId + "] Processing failed...", exception);
            return false;
        }
    }

    private String getFileExtension(String outputFormat) {
        if ("netcdf-beam".equalsIgnoreCase(outputFormat)) {
            return ".nc";
        } else if ("geotiff".equalsIgnoreCase(outputFormat)) {
            return ".tif";
        } else {
            return "";
        }
    }
}
