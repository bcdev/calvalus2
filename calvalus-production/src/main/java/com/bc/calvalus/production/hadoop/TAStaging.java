package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3Formatter;
import com.bc.calvalus.processing.l3.L3FormatterConfig;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.processing.ta.TAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.staging.Staging;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The L3 staging job.
 *
 * @author Norman
 * @author MarcoZ
 */
class TAStaging extends Staging {

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

        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }

        Logger logger = Logger.getLogger("com.bc.calvalus");
        float progress = 0f;

        WorkflowItem workflow = production.getWorkflow();
        WorkflowItem[] parallelItems = workflow.getItems();
        for (int i = 0; i < parallelItems.length; i++) {

            if (isCancelled()) {
                return null;
            }

            WorkflowItem[] sequentialItems = parallelItems[i].getItems();
            L3WorkflowItem l3WorkflowItem = (L3WorkflowItem) sequentialItems[0];
            TAWorkflowItem taWorkflowItem = (TAWorkflowItem) sequentialItems[1];

            String outputDir = taWorkflowItem.getOutputDir();
            progress += (i + 1) / parallelItems.length;
        }
        progress = 1.0f;

        return null; // ok, no error
    }

    @Override
    public void cancel() {
        super.cancel();
        FileUtils.deleteTree(stagingDir);
        production.setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
    }
}
