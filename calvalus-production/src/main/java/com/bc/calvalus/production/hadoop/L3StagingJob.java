package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3FormattingService;
import com.bc.calvalus.processing.beam.FormatterL3Config;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A staging job.
 *
 * @author MarcoZ
 */
class L3StagingJob implements Callable<L3StagingJob> {
    private final L3ProcessingRequest processingRequest;
    private final HadoopProduction production;
    private final Configuration hadoopConfiguration;
    private final String wpsXml;
    private final Logger logger;
    private float progress;

    public L3StagingJob(L3ProcessingRequest processingRequest,
                        HadoopProduction production,
                        Configuration hadoopConfiguration,
                        String wpsXml,
                        Logger logger) {
        this.processingRequest = processingRequest;
        this.production = production;
        this.hadoopConfiguration = hadoopConfiguration;
        this.wpsXml = wpsXml;
        this.logger = logger;
    }

    @Override
    public L3StagingJob call() throws Exception {
        progress = 0f;
        BeamL3FormattingService beamL3FormattingService = new BeamL3FormattingService(logger, hadoopConfiguration);
        String outputDir = processingRequest.getOutputDir();
        FormatterL3Config formatConfig = new FormatterL3Config("Product", // todo - from processingRequest
                                                               "calvalus-level3-" + production.getId() + ".dim",  // todo - from processingRequest
                                                               (String) processingRequest.getOutputFormat(),
                                                               new FormatterL3Config.BandConfiguration[0],  // todo - from processingRequest
                                                               (String) processingRequest.getProcessingParameter("startDate"),
                                                               (String) processingRequest.getProcessingParameter("stopDate"));
        try {
            beamL3FormattingService.format(formatConfig, outputDir, wpsXml);
            progress = 1f;
            production.setStagingStatus(new ProductionStatus(ProductionState.COMPLETED, progress, ""));
        } catch (Exception e) {
            production.setStagingStatus(new ProductionStatus(ProductionState.ERROR, progress, e.getMessage()));
            logger.log(Level.WARNING, "Formatting failed.", e);
        }
        return this;
    }


}
