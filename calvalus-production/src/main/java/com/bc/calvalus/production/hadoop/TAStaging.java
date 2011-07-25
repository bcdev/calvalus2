package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.binning.Aggregator;
import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.WritableVector;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.processing.ta.TAGraph;
import com.bc.calvalus.processing.ta.TAPoint;
import com.bc.calvalus.processing.ta.TAReport;
import com.bc.calvalus.processing.ta.TAResult;
import com.bc.calvalus.processing.ta.TAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionWriter;
import com.bc.calvalus.staging.Staging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.esa.beam.util.io.FileUtils;
import org.jfree.chart.JFreeChart;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The L3 staging job.
 *
 * @author Norman
 * @author MarcoZ
 */
class TAStaging extends Staging {

    public static final Logger LOGGER = Logger.getLogger("com.bc.calvalus");
    private final Production production;
    private final Configuration hadoopConfiguration;
    private final File stagingDir;

    public TAStaging(Production production,
                     Configuration hadoopConfiguration,
                     File stagingAreaPath) {
        this.production = production;
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingDir = new File(stagingAreaPath, production.getStagingPath());
    }

    @Override
    public Object call() throws Exception {

        FileSystem fs = FileSystem.get(hadoopConfiguration);

        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }

        float progress = 0f;
        int index = 0;
        production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));

        TAResult taResult = null;

        WorkflowItem workflow = production.getWorkflow();
        WorkflowItem[] parallelItems = workflow.getItems();
        for (WorkflowItem parallelItem : parallelItems) {

            if (isCancelled()) {
                return null;
            }

            WorkflowItem[] sequentialItems = parallelItem.getItems();
            L3WorkflowItem l3WorkflowItem = (L3WorkflowItem) sequentialItems[0];
            TAWorkflowItem taWorkflowItem = (TAWorkflowItem) sequentialItems[1];

            String inputDir = taWorkflowItem.getOutputDir();


            L3Config l3Config = l3WorkflowItem.getL3Config();
            BinManager binManager = l3Config.getBinningContext().getBinManager();
            if (taResult == null) {
                int aggregatorCount = binManager.getAggregatorCount();
                taResult = new TAResult(aggregatorCount);
                for (int i = 0; i < aggregatorCount; i++) {
                    Aggregator aggregator = binManager.getAggregator(i);
                    String[] outputFeatureNames = aggregator.getOutputFeatureNames();
                    taResult.setOutputFeatureNames(i, outputFeatureNames);
                }
            }

            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(inputDir, "part-r-00000"), hadoopConfiguration);
                try {
                    Text region = new Text();
                    TAPoint taPoint = new TAPoint();
                    while (reader.next(region, taPoint)) {
                        WritableVector outputVector = binManager.createOutputVector();
                        binManager.computeOutput(taPoint.getTemporalBin(), outputVector);
                        taResult.addRecord(taPoint.getRegionName(), taPoint.getStartDate(), taPoint.getStopDate(), outputVector);
                    }
                } finally {
                    reader.close();
                }
            } catch (IOException e) {
                // todo - or fail? (nf)
                production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, progress, e.getMessage()));
                LOGGER.log(Level.SEVERE, "Failed to read TA output " + inputDir, e);
            }

            clearInputDir(hadoopConfiguration, inputDir);

            index++;
            progress = (index + 1) / parallelItems.length;
            production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
        }
        TAReport taReport = new TAReport(taResult);
        TAGraph taGraph = new TAGraph(taResult);
        Set<String> regionNames = taResult.getRegionNames();
        List<String> imgUrls = new ArrayList<String>();
        for (String regionName : regionNames) {
            File regionFile = getCsvFile(regionName);
            try {
                Writer writer = new OutputStreamWriter(new FileOutputStream(regionFile));
                taReport.writeRegionCsvReport(writer, regionName);
            } catch (IOException e) {
                // todo - or fail? (nf)
                production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, progress, e.getMessage()));
                LOGGER.log(Level.SEVERE, "Failed to write TA csv file " + regionFile, e);
            }

            File pngFile = null;
            try {
                int vectorIndex = 0;
                for (int aggregatorIndex = 0; aggregatorIndex < taResult.getAggregatorCount(); aggregatorIndex++) {
                    String[] outputFeatureNames = taResult.getOutputFeatureNames(aggregatorIndex);
                    for (int i = 0; i < outputFeatureNames.length; i++) {
                        pngFile = getGraphFile(regionName, outputFeatureNames[i]);
                        JFreeChart chart = taGraph.createGRaph(regionName, aggregatorIndex, i, vectorIndex);
                        TAGraph.writeChart(chart, new FileOutputStream(pngFile));
                        imgUrls.add(pngFile.getName());
                        vectorIndex++;
                    }
                }
            } catch (IOException e) {
                // todo - or fail? (nf)
                production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, progress, e.getMessage()));
                LOGGER.log(Level.SEVERE, "Failed to write TA graph file " + (pngFile != null ? pngFile : ""), e);
            }
        }
        production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""));
        new ProductionWriter(production, imgUrls.toArray(new String[imgUrls.size()])).write(stagingDir);
        return null;
    }

    @Override
    public void cancel() {
        super.cancel();
        FileUtils.deleteTree(stagingDir);
        production.setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }

    private File getCsvFile(String regionName) {
        return new File(stagingDir, regionName + ".csv");
    }

    private File getGraphFile(String regionName, String columnName) {
        return new File(stagingDir, regionName + "_" + columnName + ".png");
    }

    private void clearInputDir(Configuration configuration, String inputDir) {
        try {
            JobUtils.clearDir(inputDir, configuration);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to delete TA output " + inputDir, e);
        }
    }
}
