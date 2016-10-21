package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.SqlStoreException;
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
class GpfTask implements Callable<Boolean> {


    private final String jobId;
    private final Map<String, Object> parameters;
    private final Product sourceProduct;
    private final File targetDir;
    private final ProcessFacade localFacade;
    private Logger logger = WpsLogger.getLogger();

    GpfTask(ProcessFacade localFacade, ProcessBuilder processBuilder) {
        this.jobId = processBuilder.getJobId();
        this.parameters = processBuilder.getParameters();
        this.sourceProduct = processBuilder.getSourceProduct();
        this.targetDir = processBuilder.getTargetDirPath().toFile();
        this.localFacade = localFacade;
    }

    @Override
    public Boolean call() throws Exception {
        LocalProductionService localProductionService = GpfProductionService.getProductionServiceSingleton();
        LocalJob job = localProductionService.getJob(jobId);
        LocalProductionStatus status = job.getStatus();
        status.setState(ProcessState.RUNNING);
        status.setProgress(10);
        try {
            logger.log(Level.INFO, "[" + jobId + "] starting subsetting operation...");
            Product subset = GPF.createProduct("Subset", parameters, sourceProduct);
            String outputFormat = (String) parameters.get("outputFormat");
            GPF.writeProduct(subset, new File(targetDir, sourceProduct.getName() + "-subset"), outputFormat,
                             false, ProgressMonitor.NULL);
            logger.log(Level.INFO, "[" + jobId + "] subsetting operation completed...");

            List<String> resultUrls = localFacade.getProductResultUrls(jobId);
            localFacade.generateProductMetadata(jobId);
            status.setState(ProcessState.COMPLETED);
            status.setProgress(100);
            status.setResultUrls(resultUrls);
            status.setStopDate(new Date());
            job.updateStatus(status);
            localProductionService.updateJob(job);
            return true;
        } catch (OperatorException exception) {
            status.setState(ProcessState.ERROR);
            status.setMessage("GPF process failed : " + exception.getMessage());
            status.setStopDate(new Date());
            job.updateStatus(status);
            localProductionService.updateJob(job);
            logger.log(Level.SEVERE, "[" + jobId + "] GPF process failed...", exception);
            return false;
        } catch (ProductMetadataException exception) {
            status.setState(ProcessState.ERROR);
            status.setMessage("Creating product metadata failed : " + exception.getMessage());
            status.setStopDate(new Date());
            job.updateStatus(status);
            localProductionService.updateJob(job);
            logger.log(Level.SEVERE, "[" + jobId + "] Creating product metadata failed...", exception);
            return false;
        } catch (Exception exception) {
            status.setState(ProcessState.ERROR);
            status.setMessage("Processing failed : " + exception.getMessage());
            status.setStopDate(new Date());
            job.updateStatus(status);
            localProductionService.updateJob(job);
            logger.log(Level.SEVERE, "[" + jobId + "] Processing failed...", exception);
            return false;
        } finally {
            try {
                localProductionService.updateStatuses();
            } catch (SqlStoreException exception) {
                logger.log(Level.SEVERE, "[" + jobId + "] Unable to persist the job information to DB...", exception);
            }
        }
    }
}
