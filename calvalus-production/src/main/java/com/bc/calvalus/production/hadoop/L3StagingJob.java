package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3FormattingService;
import com.bc.calvalus.processing.beam.FormatterL3Config;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.conf.Configuration;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.File;

/**
 * A staging job.
 *
 * @author MarcoZ
 */
class L3StagingJob extends StagingJob {
    private final HadoopProduction production;
    private final L3ProcessingRequest[] processingRequests;
    private final Configuration hadoopConfiguration;
    private final String stagingDir;
    private final String outputFormat;
    private float progress;

    public L3StagingJob(HadoopProduction production,
                        L3ProcessingRequest[] processingRequests,
                        Configuration hadoopConfiguration,
                        String stagingDir) {
        this.production = production;
        this.processingRequests = processingRequests;
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingDir = stagingDir;
        this.outputFormat = production.getProductionRequest().getProductionParameter("outputFormat");
    }

    @Override
    public L3StagingJob call() throws Exception {
        Logger logger = Logger.getLogger("com.bc.calvalus");
        progress = 0f;
        for (int i = 0; i < processingRequests.length; i++) {
            L3ProcessingRequest processingRequest = processingRequests[i];

            BeamL3FormattingService beamL3FormattingService = new BeamL3FormattingService(logger, hadoopConfiguration);
            String outputDir = processingRequest.getOutputDir();
            FormatterL3Config formatConfig = new FormatterL3Config("Product", // todo - from processingRequest
                                                                   new File(stagingDir, production.getId()).getPath(),
                                                                   processingRequest.getOutputFormat(),
                                                                   new FormatterL3Config.BandConfiguration[0],  // todo - from processingRequest
                                                                   (String) processingRequest.getProcessingParameter("dateStart"),
                                                                   (String) processingRequest.getProcessingParameter("dateStop"));
            try {
                // todo - need a progress monitor here
                beamL3FormattingService.format(formatConfig, outputDir, "");
                progress = 1f;
                // todo - if job has been cancelled, it must not change its state anymore
                production.setStagingStatus(new ProductionStatus(ProductionState.COMPLETED, progress, ""));
            } catch (Exception e) {
                // todo - if job has been cancelled, it must not change its state anymore
                production.setStagingStatus(new ProductionStatus(ProductionState.ERROR, progress, e.getMessage()));
                logger.log(Level.WARNING, "Formatting failed.", e);
            }
            progress += (i + 1) / processingRequests.length;
        }
        progress = 1.0f;

        return this;
    }

    @Override
    public boolean isCancelled() {
        return production.getStagingStatus().getState() == ProductionState.CANCELLED;
    }

    @Override
    public void cancel() {
        // todo - cleanup output directory!!!
        production.setStagingStatus(new ProductionStatus(ProductionState.CANCELLED));
    }
}
