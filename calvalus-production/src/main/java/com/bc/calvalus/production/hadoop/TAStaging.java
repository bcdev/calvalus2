package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.WritableVector;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.processing.ta.TAPoint;
import com.bc.calvalus.processing.ta.TAResult;
import com.bc.calvalus.processing.ta.TAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.staging.Staging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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

        TAResult taResult = new TAResult();

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
            taResult.setOutputFeatureNames(binManager.getAggregator(0).getOutputFeatureNames());

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
                LOGGER.log(Level.SEVERE, "Failed to read TA output " + inputDir, e);
            }

            clearInputDir(hadoopConfiguration, inputDir);
        }

        Set<String> regionNames = taResult.getRegionNames();
        for (String regionName : regionNames) {
            try {
                writeRegionFile(taResult, regionName);
            } catch (IOException e) {
                // todo - or fail? (nf)
                LOGGER.log(Level.SEVERE, "Failed to write TA region file " + getRegionFile(regionName), e);
            }
        }

        return null;
    }

    @Override
    public void cancel() {
        super.cancel();
        FileUtils.deleteTree(stagingDir);
        production.setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }

    private void writeRegionFile(TAResult taResult, String regionName) throws IOException {
        File path = getRegionFile(regionName);
        Writer writer = new FileWriter(path);
        try {
            writeHeader(writer, taResult);
            List<TAResult.Record> records = taResult.getRecords(regionName);
            for (TAResult.Record record : records) {
                writeRecord(writer, record);
            }
        } finally {
            writer.close();
        }
    }

    private File getRegionFile(String regionName) {
        return new File(stagingDir, regionName + ".csv");
    }

    private void writeRecord(Writer fsDataOutputStream, TAResult.Record record) throws IOException {
        fsDataOutputStream.write(record.startDate);
        fsDataOutputStream.write("\t");
        fsDataOutputStream.write(record.stopDate);
        for (int i = 0; i < record.outputVector.size(); i++) {
            fsDataOutputStream.write("\t");
            fsDataOutputStream.write(record.outputVector.get(i) + "");
        }
    }

    private void writeHeader(Writer fsDataOutputStream, TAResult taResult) throws IOException {
        List<String> header = taResult.getHeader();
        for (String s : header) {
            fsDataOutputStream.write(s);
            fsDataOutputStream.write("\t");
        }
        fsDataOutputStream.write("\n");
    }

    private void clearInputDir(Configuration configuration, String inputDir) {
        try {
            JobUtils.clearDir(inputDir, configuration);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to delete TA output " + inputDir, e);
        }
    }
}
