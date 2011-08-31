package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.ma.MAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.staging.Staging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * The staging job for match-up analysis (MA) results.
 *
 * @author Norman
 */
class MAStaging extends Staging {

    private final Production production;
    private final Configuration hadoopConfiguration;
    private final File stagingDir;

    public MAStaging(Production production,
                     Configuration hadoopConfiguration,
                     File stagingAreaPath) {
        this.production = production;
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingDir = new File(stagingAreaPath, production.getStagingPath());
    }

    @Override
    public Object call() throws Exception {
        production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, 0.0F, ""));
        MAWorkflowItem workflow = (MAWorkflowItem) production.getWorkflow();
        Path remoteDataFile = new Path(workflow.getOutputDir(), "part-r-00000");
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }

        try {
            FileSystem fileSystem = remoteDataFile.getFileSystem(hadoopConfiguration);

            /*
            Text key = new Text();
            RecordWritable value = new RecordWritable();
            SequenceFile.Reader dataReader = new SequenceFile.Reader(fileSystem, remoteDataFile, hadoopConfiguration);
            while (dataReader.next(key, value)) {

            }
            */

            FileUtil.copy(fileSystem, remoteDataFile, new File(stagingDir, "ma-result.csv"), false, hadoopConfiguration);
            production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, 0.5F, ""));

            FileStatus[] imageFileStatuses = fileSystem.globStatus(new Path(workflow.getOutputDir(), "*.png"));
            Path[] imageFilePaths = FileUtil.stat2Paths(imageFileStatuses);
            if (imageFilePaths != null) {
                for (int i = 0; i < imageFilePaths.length; i++) {
                    Path remoteImageFile = imageFilePaths[i];
                    FileUtil.copy(fileSystem, remoteImageFile, new File(stagingDir, remoteImageFile.getName()),
                                  false, hadoopConfiguration);
                    production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, 0.5F + (0.5F * i) / imageFilePaths.length, ""));
                }
            }

            zip(stagingDir, new File(stagingDir.getParentFile(), stagingDir.getName() + ".zip"));
            // FileUtil.fullyDelete();

            production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0F, ""));

        } catch (IOException e) {
            production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, 1.0F, e.getMessage()));
        }

        return null;
    }

    @Override
    public void cancel() {
        super.cancel();
        FileUtils.deleteTree(stagingDir);
        production.setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }
}
