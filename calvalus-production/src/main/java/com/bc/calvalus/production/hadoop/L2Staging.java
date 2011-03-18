package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.processing.beam.L2WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.staging.Staging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

import java.io.File;

/**
 * The L2 staging job.
 *
 * @author MarcoZ
 */
class L2Staging extends Staging {

    private final Production production;
    private final Configuration hadoopConfiguration;
    private final File stagingDir;

    public L2Staging(Production production,
                     Configuration hadoopConfiguration,
                     File stagingAreaPath) {
        this.production = production;
        this.hadoopConfiguration = hadoopConfiguration;
        this.stagingDir = new File(stagingAreaPath, production.getStagingPath());
    }

    @Override
    public String call() throws Exception {
        L2WorkflowItem workflow = (L2WorkflowItem) production.getWorkflow();
        String outputDir = workflow.getOutputDir();

        float progress = 0f;
        production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
        try {
            Path outputPath = new Path(outputDir);
            FileSystem fileSystem = outputPath.getFileSystem(hadoopConfiguration);
            FileStatus[] seqFiles = fileSystem.listStatus(outputPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().endsWith(".seq");
                }
            });
            if (!stagingDir.exists()) {
                stagingDir.mkdirs();
            }
            int index = 0;
            for (FileStatus seqFile : seqFiles) {
                Path seqProductPath = seqFile.getPath();
                StreamingProductReader reader = new StreamingProductReader(seqProductPath, hadoopConfiguration);
                Product product = reader.readProductNodes(null, null);
                String dimapProductName = seqProductPath.getName().replaceFirst(".seq", ".dim");   // todo - make use of "outputFormat" parameter
                File productFile = new File(stagingDir, dimapProductName);
                ProductIO.writeProduct(product, productFile, ProductIO.DEFAULT_FORMAT_NAME, false);
                index++;
                progress = (index + 1) / seqFiles.length;
                production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
            }
            production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""));
            // todo - zip or tar.gz all output DIMAPs to outputPath.getName() + ".zip" and remove outputPath.getName()
        } catch (Exception e) {
            production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, production.getStagingStatus().getProgress(), e.getMessage()));
            throw new ProductionException("Error: " + e.getMessage(), e);
        }
        return null;
    }
}
