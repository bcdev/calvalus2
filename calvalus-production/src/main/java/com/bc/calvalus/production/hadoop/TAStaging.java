package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.ta.TAGraph;
import com.bc.calvalus.processing.ta.TAPoint;
import com.bc.calvalus.processing.ta.TAReport;
import com.bc.calvalus.processing.ta.TAResult;
import com.bc.calvalus.processing.ta.TAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionStaging;
import com.bc.calvalus.production.ProductionWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.esa.beam.binning.Aggregator;
import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.WritableVector;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.util.io.FileUtils;
import org.jfree.chart.JFreeChart;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The TA staging job.
 *
 * @author Norman
 * @author MarcoZ
 * @author Martin
 */
class TAStaging extends ProductionStaging {

    public static final Logger LOGGER = Logger.getLogger("com.bc.calvalus");
    private static final long GIGABYTE = 1024L * 1024L * 1024L;
    private final Configuration hadoopConfiguration;
    private final File stagingDir;

    public TAStaging(Production production,
                     Configuration hadoopConfiguration,
                     File stagingAreaPath) {
        super(production);
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingDir = new File(stagingAreaPath, production.getStagingPath());
    }

    @Override
    public void performStaging() throws Throwable {

        final Production production = getProduction();
        float progress = 0f;
        int index = 0;
        production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));

        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }

        final WorkflowItem workflow = production.getWorkflow();
        final TAWorkflowItem taWorkflowItem = (TAWorkflowItem) workflow.getItems()[workflow.getItems().length - 1];
        final String inputDir = taWorkflowItem.getOutputDir();

        final ProductionRequest productionRequest = production.getProductionRequest();
        final String l3ConfigXml = L3ProductionType.getL3ConfigXml(productionRequest);
        BinningConfig binningConfig = BinningConfig.fromXml(l3ConfigXml);
        final BinManager binManager = HadoopBinManager.createBinningContext(binningConfig, null).getBinManager();

        final List<String> outputFeatureNames = new ArrayList<String>();
        final int aggregatorCount = binManager.getAggregatorCount();
        for (int i = 0; i < aggregatorCount; i++) {
            final Aggregator aggregator = binManager.getAggregator(i);
            outputFeatureNames.addAll(Arrays.asList(aggregator.getOutputFeatureNames()));
        }

        final Text regionDateKey = new Text();
        final TAPoint taPoint = new TAPoint();
        final TAResult taResult = new TAResult();
        taResult.setOutputFeatureNames(outputFeatureNames.toArray(new String[outputFeatureNames.size()]));

        FileSystem fs = FileSystem.get(hadoopConfiguration);
        try {
            final FileStatus[] fileStatuses = fs.globStatus(new Path(inputDir, "part-r-?????"));
            for (FileStatus file : fileStatuses) {
                final SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), hadoopConfiguration);
                try {
                    while (reader.next(regionDateKey, taPoint)) {
                        final WritableVector outputVector = binManager.createOutputVector();
                        binManager.computeOutput(taPoint.getTemporalBin(), outputVector);
                        taResult.addRecord(taPoint.getRegionName(), taPoint.getStartDate(), taPoint.getStopDate(), outputVector);
                    }
                } finally {
                    reader.close();
                }
                if (isCancelled()) {
                    return;
                }
                index++;
                progress = 1.0f - (1.0f / (index + 1));
                production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
            }
        } catch (IOException e) {
            production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, progress, e.getMessage()));
            LOGGER.log(Level.SEVERE, "Failed to read TA output " + inputDir, e);
        }

        final TAReport taReport = new TAReport(taResult);
        final TAGraph taGraph = new TAGraph(taResult);
        final Set<String> regionNames = taResult.getRegionNames();
        final List<String> imgUrls = new ArrayList<String>();
        for (String regionName : regionNames) {
            File regionFile = getCsvFile(regionName);
            try {
                final Writer writer = new OutputStreamWriter(new FileOutputStream(regionFile));
                taReport.writeRegionCsvReport(writer, regionName);
            } catch (IOException e) {
                production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, progress, e.getMessage()));
                LOGGER.log(Level.SEVERE, "Failed to write TA csv file " + regionFile, e);
            }

            File pngFile = null;
            try {
                String[] featureNames = taResult.getOutputFeatureNames();
                for (int featureIndex = 0; featureIndex < featureNames.length; featureIndex++) {
                    pngFile = new File(stagingDir, "Yearly_cycle-" + regionName + "-" + featureNames[featureIndex] + ".png");
                    JFreeChart chart = taGraph.createYearlyCyclGaph(regionName, featureIndex);
                    TAGraph.writeChart(chart, new FileOutputStream(pngFile));
                    imgUrls.add(pngFile.getName());

                    pngFile = new File(stagingDir, "Timelseries-" + regionName + "-" + featureNames[featureIndex] + ".png");
                    int sigmaIndex = findSigmaFeature(featureNames, featureNames[featureIndex]);
                    if (sigmaIndex != -1) {
                        chart = taGraph.createTimeseriesSigmaGraph(regionName, featureIndex, sigmaIndex);
                    } else {
                        chart = taGraph.createTimeseriesGaph(regionName, featureIndex);
                    }
                    TAGraph.writeChart(chart, new FileOutputStream(pngFile));
                    imgUrls.add(pngFile.getName());

                    pngFile = new File(stagingDir, "Anomaly-" + regionName + "-" + featureNames[featureIndex] + ".png");
                    chart = taGraph.createAnomalyGraph(regionName, featureIndex);
                    TAGraph.writeChart(chart, new FileOutputStream(pngFile));
                    imgUrls.add(pngFile.getName());

                }
            } catch (IOException e) {
                production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, progress, e.getMessage()));
                LOGGER.log(Level.SEVERE, "Failed to write TA graph file " + (pngFile != null ? pngFile : ""), e);
            }
        }

        final FileStatus[] fileStatuses = fs.globStatus(new Path(inputDir, "*-timeseries.csv"));
        long totalFilesSize = 0L;
        if (fileStatuses != null) {
            for (int i = 0; i < fileStatuses.length; i++) {
                FileStatus fileStatus = fileStatuses[i];
                Path path = fileStatus.getPath();
                LOGGER.info("Copying " + path.getName() + " to staging area ...");
                FileUtil.copy(fs,
                              path,
                              new File(stagingDir, path.getName()),
                              false, hadoopConfiguration);
                totalFilesSize += fileStatus.getLen();
                production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, (i + 1.0F) / fileStatuses.length, path.getName()));
            }
        }
        if (totalFilesSize < 2L * GIGABYTE / 2) {
            String zipFilename = getSafeFilename(production.getName() + ".zip");
            zip(stagingDir, new File(stagingDir, zipFilename));
        }

        production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""));

        // handle umlaut in lake names
        for (int i=0; i<imgUrls.size(); ++i) {
            imgUrls.set(i, URLEncoder.encode(imgUrls.get(i), "UTF-8"));
        }
        new ProductionWriter(production, imgUrls.toArray(new String[imgUrls.size()])).write(stagingDir);
    }

    private int findSigmaFeature(String[] featureNames, String featureName) {
        int sigmaIndex = -1;
        if (featureName.endsWith("_mean")) {
            String sigmaName = featureName.substring(0, featureName.length() - "_mean".length()) + "_sigma";
            for (int i=0; i<featureNames.length; ++i) {
                if (featureNames[i].equals(sigmaName)) {
                    sigmaIndex = i;
                    break;
                }
            }
        }
        return sigmaIndex;
    }

    @Override
    public void cancel() {
        super.cancel();
        FileUtils.deleteTree(stagingDir);
        getProduction().setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }

    private File getCsvFile(String regionName) {
        return new File(stagingDir, regionName + ".csv");
    }

    private void clearInputDir(Configuration configuration, String inputDir) {
        try {
            JobUtils.clearDir(inputDir, configuration);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to delete TA output " + inputDir, e);
        }
    }
}
