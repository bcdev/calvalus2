package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.beam.L3Config;
import com.bc.calvalus.processing.beam.L3Formatter;
import com.bc.calvalus.processing.beam.L3FormatterConfig;
import com.bc.calvalus.processing.hadoop.L3ProcessingRequest;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.staging.Staging;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
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
    private final File stagingAreaPath;
    private float progress;

    public L3Staging(Production production,
                     L3ProcessingRequest[] processingRequests,
                     Configuration hadoopConfiguration, File stagingAreaPath) {
        this.production = production;
        this.processingRequests = processingRequests;
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingAreaPath = stagingAreaPath;
    }

    @Override
    public String call() throws Exception {
        Logger logger = Logger.getLogger("com.bc.calvalus");
        L3Config l3config = processingRequests[0].getBeamL3Config();
        progress = 0f;
        for (int i = 0; i < processingRequests.length; i++) {
            L3ProcessingRequest processingRequest = processingRequests[i];
            L3FormatterConfig formatterConfig = processingRequest.getFormatterL3Config(new File(stagingAreaPath, production.getStagingPath()).getPath());
            String outputDir = processingRequest.getOutputDir();

            if (isCancelled()) {
                return null;
            }

            L3Formatter formatter = new L3Formatter(logger, hadoopConfiguration);
            try {
                // todo - need a progress monitor here
                formatter.format(formatterConfig, l3config, outputDir);
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
    public void cancel() {
        super.cancel();
        // todo - cleanup output directory!!!
        production.setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }
}
