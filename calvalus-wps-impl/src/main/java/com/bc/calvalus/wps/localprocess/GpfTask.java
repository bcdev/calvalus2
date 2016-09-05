package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.utils.ProductMetadata;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.WpsLogger;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.OperatorException;

import java.io.File;
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
    private final String requestUrl;
    private Logger logger = WpsLogger.getLogger();

    public GpfTask(String jobId, Map<String, Object> parameters, Product sourceProduct, File targetDir, String hostName, int portNumber, String requestUrl) {
        this.jobId = jobId;
        this.parameters = parameters;
        this.sourceProduct = sourceProduct;
        this.targetDir = targetDir;
        this.hostName = hostName;
        this.portNumber = portNumber;
        this.requestUrl = requestUrl;
    }

    @Override
    public Boolean call() throws Exception {
        ProductionStatus status = GpfProductionService.getProductionStatusMap().get(jobId);
        status.setState(ProductionState.RUNNING);
        status.setProgress(10);
        try {
            logger.log(Level.INFO, "[" + jobId + "] starting subsetting operation...");
            Product subset = GPF.createProduct("Subset", parameters, sourceProduct);
            GPF.writeProduct(subset, new File(targetDir, sourceProduct.getName() + ".nc"), "Netcdf-BEAM", false, ProgressMonitor.NULL);
            logger.log(Level.INFO, "[" + jobId + "] subsetting operation completed...");

            LocalStaging staging = new LocalStaging();
            List<String> resultUrls = staging.getProductUrls(hostName, portNumber, targetDir, jobId);
            staging.generateProductMetadata(targetDir, jobId, parameters, new LocalSubsetProcessor());
            status.setState(ProductionState.SUCCESSFUL);
            status.setProgress(100);
            status.setResultUrls(resultUrls);
            GpfProductionService.getProductionStatusMap().put(jobId, status);
            return true;
        } catch (OperatorException exception) {
            status.setState(ProductionState.FAILED);
            status.setMessage("GPF process failed : " + exception.getMessage());
            GpfProductionService.getProductionStatusMap().put(jobId, status);
            logger.log(Level.SEVERE, "[" + jobId + "] GPF process failed...", exception);
            return false;
        } catch (ProductMetadataException exception) {
            status.setState(ProductionState.FAILED);
            status.setMessage("Creating product metadata failed : " + exception.getMessage());
            GpfProductionService.getProductionStatusMap().put(jobId, status);
            logger.log(Level.SEVERE, "[" + jobId + "] Creating product metadata failed...", exception);
            return false;
        } catch (Exception exception) {
            status.setState(ProductionState.FAILED);
            status.setMessage("Processing failed : " + exception.getMessage());
            GpfProductionService.getProductionStatusMap().put(jobId, status);
            logger.log(Level.SEVERE, "[" + jobId + "] Processing failed...", exception);
            return false;
        }

    }
}
