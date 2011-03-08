package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.processing.beam.BeamL3FormattingService;
import com.bc.calvalus.processing.beam.FormatterL3Config;
import com.bc.calvalus.production.ProcessState;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProcessStatus;
import com.bc.calvalus.production.Staging;
import org.apache.hadoop.conf.Configuration;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A staging job.
 *
 * @author MarcoZ
 */
class L3Staging extends Staging {
    private final Production production;
    private final L3ProcessingRequest[] processingRequests;
    private final Configuration hadoopConfiguration;
    private float progress;

    public L3Staging(Production production,
                     L3ProcessingRequest[] processingRequests,
                     Configuration hadoopConfiguration) {
        this.production = production;
        this.processingRequests = processingRequests;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @Override
    public Void call() throws Exception {
        Logger logger = Logger.getLogger("com.bc.calvalus");
        BeamL3Config beamL3config = processingRequests[0].getBeamL3Config();
        progress = 0f;
        for (int i = 0; i < processingRequests.length; i++) {
            L3ProcessingRequest processingRequest = processingRequests[i];
            FormatterL3Config formatConfig = processingRequest.getFormatterL3Config();

            BeamL3FormattingService beamL3FormattingService = new BeamL3FormattingService(logger, hadoopConfiguration);
            String outputDir = processingRequest.getOutputDir();
            try {
                // todo - need a progress monitor here
                beamL3FormattingService.format(formatConfig, beamL3config, outputDir);
                progress = 1f;
                // todo - if job has been cancelled, it must not change its state anymore
                production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, progress, ""));
            } catch (Exception e) {
                // todo - if job has been cancelled, it must not change its state anymore
                production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, progress, e.getMessage()));
                logger.log(Level.WARNING, "Formatting failed.", e);
            }
            progress += (i + 1) / processingRequests.length;
        }
        progress = 1.0f;

        return null;
    }

    @Override
    public boolean isCancelled() {
        return production.getStagingStatus().getState() == ProcessState.CANCELLED;
    }

    @Override
    public void cancel() {
        // todo - cleanup output directory!!!
        production.setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }
}
