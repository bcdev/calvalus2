package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.ma.MAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.staging.Staging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * The staging job for match-up analysis (MA) results.
 *
 * @author Norman
 */
class MAStaging extends Staging {

    public static final Logger LOGGER = Logger.getLogger("com.bc.calvalus");
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
        Path remoteFile = new Path(workflow.getOutputDir(), "part-r-00000");
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }
        try {
            FileSystem fileSystem = remoteFile.getFileSystem(hadoopConfiguration);
            FileUtil.copy(fileSystem, remoteFile, new File(stagingDir.getCanonicalPath(), "ma-result.csv"), false, hadoopConfiguration);
            production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""));
        } catch (IOException e) {
            production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, 1.0f, e.getMessage()));
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
