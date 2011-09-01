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
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }

        Path remoteOutputDir = new Path(workflow.getOutputDir());
        FileSystem fileSystem = remoteOutputDir.getFileSystem(hadoopConfiguration);

        // Simply copy entire content of remoteOutputDir
        try {
            FileStatus[] fileStatuses = fileSystem.globStatus(new Path(remoteOutputDir, "*.*"));
            Path[] paths = FileUtil.stat2Paths(fileStatuses);
            if (paths != null) {
                for (int i = 0; i < paths.length; i++) {
                    Path path = paths[i];
                    FileUtil.copy(fileSystem,
                                  path,
                                  new File(stagingDir, path.getName()),
                                  false, hadoopConfiguration);
                    production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, (i + 1.0F) / paths.length, path.getName()));
                }
            }

            zip(stagingDir, new File(stagingDir.getParentFile(), stagingDir.getName() + ".zip"));
            // todo FileUtil.fullyDelete();

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
