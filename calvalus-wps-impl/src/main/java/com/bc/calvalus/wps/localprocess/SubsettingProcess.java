package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.utilities.WpsLogger;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.OperatorException;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class SubsettingProcess implements Process {

    private Logger logger = WpsLogger.getLogger();

    @Override
    public LocalProductionStatus processAsynchronous(ProcessBuilder processBuilder) {
        logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] starting asynchronous process...");
        LocalProductionStatus status = new LocalProductionStatus(processBuilder.getJobId(),
                                                                 ProductionState.ACCEPTED,
                                                                 0,
                                                                 "The request has been queued.",
                                                                 null);
        GpfProductionService.getProductionStatusMap().put(processBuilder.getJobId(), status);
        GpfTask gpfTask = new GpfTask(processBuilder.getJobId(),
                                      processBuilder.getParameters(),
                                      processBuilder.getSourceProduct(),
                                      processBuilder.getTargetDirPath().toFile(),
                                      processBuilder.getServerContext().getHostAddress(),
                                      processBuilder.getServerContext().getPort());
        GpfProductionService.getWorker().submit(gpfTask);
        logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] job has been queued...");
        return status;
    }

    @Override
    public LocalProductionStatus processSynchronous(ProcessBuilder processBuilder) {
        LocalProductionStatus status;
        try {
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] starting synchronous process...");
            Product subset = GPF.createProduct("Subset", processBuilder.getParameters(), processBuilder.getSourceProduct());
            GPF.writeProduct(subset, new File(processBuilder.getTargetDirPath().toFile(), processBuilder.getSourceProduct().getName() + ".nc"), "Netcdf-BEAM", false, ProgressMonitor.NULL);
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] constructing result URLs...");
            LocalStaging staging = new LocalStaging();
            List<String> resultUrls = staging.getProductUrls(processBuilder.getServerContext().getHostAddress(),
                                                             processBuilder.getServerContext().getPort(),
                                                             processBuilder.getTargetDirPath().toFile(),
                                                             processBuilder.getJobId());
            staging.generateProductMetadata(processBuilder.getTargetDirPath().toFile(),
                                            processBuilder.getJobId(),
                                            processBuilder.getParameters(),
                                            new LocalSubsetProcessor(),
                                            processBuilder.getServerContext().getHostAddress(),
                                            processBuilder.getServerContext().getPort());
            logger.log(Level.INFO, "[" + processBuilder.getJobId() + "] job has been completed, creating successful response...");
            status = new LocalProductionStatus(processBuilder.getJobId(),
                                               ProductionState.SUCCESSFUL,
                                               100,
                                               "The request has been processed successfully.",
                                               resultUrls);
            status.setStopDate(new Date());
            return status;
        } catch (OperatorException | ProductionException exception) {
            status = new LocalProductionStatus(processBuilder.getJobId(),
                                               ProductionState.FAILED,
                                               0,
                                               "Processing failed : " + exception.getMessage(),
                                               null);
            status.setStopDate(new Date());
            return status;
        } catch (ProductMetadataException exception) {
            String jobId = processBuilder.getJobId();
            status = new LocalProductionStatus(jobId,
                                               ProductionState.FAILED,
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
    public ExecuteResponse createLineageAsyncExecuteResponse(LocalProductionStatus status, ProcessBuilder processBuilder) {
        CalvalusExecuteResponseConverter executeAcceptedResponse = new CalvalusExecuteResponseConverter();
        List<DocumentOutputDefinitionType> outputType = processBuilder.getExecuteRequest().getResponseForm().getResponseDocument().getOutput();
        return executeAcceptedResponse.getAcceptedWithLineageResponse(status.getJobId(), processBuilder.getExecuteRequest().getDataInputs(),
                                                                      outputType, processBuilder.getServerContext());
    }

    @Override
    public ExecuteResponse createLineageSyncExecuteResponse(LocalProductionStatus status, ProcessBuilder processBuilder) {
        CalvalusExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
        List<DocumentOutputDefinitionType> outputType = processBuilder.getExecuteRequest().getResponseForm().getResponseDocument().getOutput();
        return executeSuccessfulResponse.getSuccessfulWithLineageResponse(status.getResultUrls(), processBuilder.getExecuteRequest().getDataInputs(), outputType);
    }
}
