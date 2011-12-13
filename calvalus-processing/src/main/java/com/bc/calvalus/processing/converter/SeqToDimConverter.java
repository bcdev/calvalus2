package com.bc.calvalus.processing.converter;

import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.processing.shellexec.ProcessUtil;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

import java.io.File;
import java.io.IOException;

/**
 *
 *
 * @author Boe
 */
@Deprecated
public class SeqToDimConverter implements FormatConverter {

    private static final String TMP_DIR = "/home/hadoop/tmp";
    private static final String ARCHIVE_ROOT_DIR = "/mnt/hdfs";

    public void convert(String taskId, Path inputPath, String outputDir, String targetFormat, Configuration configuration)
        throws IOException
    {
        StreamingProductReader reader = new StreamingProductReader(inputPath, configuration);
        Product product = reader.readProductNodes(null, null);

        final File tmpDir = new File(TMP_DIR + File.separator + taskId);
        tmpDir.mkdirs();
        final String outputFilename = inputPath.getName().replace(".seq", ".dim");
        ProductIO.writeProduct(product, new File(tmpDir, outputFilename).getPath(), ProductIO.DEFAULT_FORMAT_NAME);

        final ProcessUtil copyProcess = new ProcessUtil();
        copyProcess.directory(tmpDir);
        try {
            final String outputPath = (outputDir.startsWith("hdfs:"))
                    ? ARCHIVE_ROOT_DIR + File.separator + new Path(outputDir).toUri().getPath()
                    : (outputDir.startsWith("file:"))
                    ? new Path(outputDir).toUri().getPath()
                    : outputDir;
            final int returnCode = copyProcess.run("/bin/bash", "-c", "mkdir -p " + outputPath + "; cp -r * " + outputPath + "; rm -rf " + tmpDir.getPath());
            if (returnCode != 0) {
                throw new ProcessorException("execution of cp -r * " + outputPath + " failed: " + copyProcess.getOutputString());
            }
        } catch (InterruptedException e) {
            throw new ProcessorException("copying result to " + outputDir + " failed: " + e);
        }
    }
}

