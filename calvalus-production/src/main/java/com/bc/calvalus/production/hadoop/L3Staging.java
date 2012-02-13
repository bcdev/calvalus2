package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3Formatter;
import com.bc.calvalus.processing.l3.L3FormatterConfig;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionStaging;
import com.bc.calvalus.production.ProductionWriter;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.OutputterConfig;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The L3 staging job.
 *
 * @author MarcoZ
 */
class L3Staging extends ProductionStaging {

    private final Configuration hadoopConfiguration;
    private final File stagingDir;

    public L3Staging(Production production,
                     Configuration hadoopConfiguration,
                     File stagingAreaPath) {
        super(production);
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingDir = new File(stagingAreaPath, production.getStagingPath());
    }

    @Override
    public void performStaging() throws Throwable {
        Production production = getProduction();
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }

        Geometry regionGeometry = production.getProductionRequest().getRegionGeometry(null);

        Logger logger = Logger.getLogger("com.bc.calvalus");
        float progress = 0f;

        WorkflowItem workflow = production.getWorkflow();
        WorkflowItem[] items = workflow.getItems();
        for (int i = 0; i < items.length; i++) {

            if (isCancelled()) {
                return;
            }
            L3WorkflowItem l3WorkflowItem = (L3WorkflowItem) items[i];
            String outputDir = l3WorkflowItem.getOutputDir();
            L3Config l3Config = l3WorkflowItem.getL3Config();


            L3Formatter formatter = new L3Formatter(hadoopConfiguration);
            try {
                File tmpDir = new File(stagingDir, "tmp");
                try {
                    tmpDir.mkdir();
                    L3FormatterConfig formatterConfig = createFormatterConfig(tmpDir,
                                                                              l3WorkflowItem.getMinDate(),
                                                                              l3WorkflowItem.getMaxDate());

                    formatter.format(formatterConfig, l3Config, outputDir, regionGeometry);

                    String outputFilename = new File(formatterConfig.getOutputFile()).getName();
                    String zipFileName = FileUtils.exchangeExtension(outputFilename, ".zip");
                    zip(tmpDir, new File(stagingDir, zipFileName));
                } finally {
                  FileUtils.deleteTree(tmpDir);
                }
                production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
            } catch (Exception e) {
                // check: if job has been cancelled, it must not change its state anymore (nf)
                production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, progress, e.getMessage()));
                logger.log(Level.WARNING, "Formatting failed.", e);
            }
            progress += (i + 1) / items.length;
        }
        progress = 1.0f;
        production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, progress, ""));
        new ProductionWriter(production).write(stagingDir);
    }

    private L3FormatterConfig createFormatterConfig(File outputDir, String dateStart, String dateStop) throws ProductionException {
        Production production = getProduction();

        String outputFormat = production.getProductionRequest().getString("outputFormat");
        String extension;
        if (outputFormat.equals("BEAM-DIMAP")) {
            extension = "dim";
        } else if (outputFormat.equals("NetCDF")) {
            extension = "nc";
        } else if (outputFormat.equals("GeoTIFF")) {
            extension = "tif";
        } else {
            extension = "xxx"; // todo - what else to handle? (mz)
        }
        // todo - specify common Calvalus L3 filename convention (mz)
        String filename = String.format("L3_%s_%s.%s", dateStart, dateStop, extension);
        String stagingFilePath = new File(outputDir, filename).getPath();

        // todo - make 'outputType' a production request parameter (mz)
        String outputType = "Product";
        // todo - make 'bandConfiguration' a production request parameter (mz)
        OutputterConfig.BandConfiguration[] rgbBandConfig = new OutputterConfig.BandConfiguration[0];

        return new L3FormatterConfig(outputType,
                                     stagingFilePath,
                                     outputFormat,
                                     rgbBandConfig,
                                     dateStart,
                                     dateStop);
    }

    @Override
    public void cancel() {
        super.cancel();
        FileUtils.deleteTree(stagingDir);
        getProduction().setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }
}
