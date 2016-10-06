package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.wps.utilities.WpsLogger;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class LocalProduction {

    private Logger logger = WpsLogger.getLogger();

    public LocalProductionStatus orderProductionAsynchronous(LocalFacade localFacade, ProcessBuilder processBuilder) {
        logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] starting asynchronous process...");
        LocalProductionStatus status = new LocalProductionStatus(processBuilder.getJobId(),
                                                                 ProcessState.SCHEDULED,
                                                                 0,
                                                                 "The request has been queued.",
                                                                 null);
        LocalJob job = new LocalJob(processBuilder.getJobId(), processBuilder.getParameters(), status);
        GpfProductionService.getProductionStatusMap().put(processBuilder.getJobId(), job);
        GpfTask gpfTask = new GpfTask(localFacade,
                                      processBuilder.getServerContext().getHostAddress(),
                                      processBuilder.getServerContext().getPort(),
                                      processBuilder);
        GpfProductionService.getWorker().submit(gpfTask);
        logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] job has been queued...");
        return status;
    }

    public void orderProductionSynchronous(ProcessBuilder processBuilder)
                throws InvalidProcessorIdException, BindingException, IOException,
                       URISyntaxException, ProductionException, ProductMetadataException {
        logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] starting synchronous process...");
        Product subset = GPF.createProduct("Subset", processBuilder.getParameters(), processBuilder.getSourceProduct());
        String outputFormat = (String) processBuilder.getParameters().get("outputFormat");
        GPF.writeProduct(subset,
                         new File(processBuilder.getTargetDirPath().toFile(), processBuilder.getSourceProduct().getName() + getFileExtension(outputFormat)),
                         outputFormat,
                         false,
                         ProgressMonitor.NULL);
        logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] process finished...");
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
