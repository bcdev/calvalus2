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
public class SeqToDimConverter implements FormatConverter {

    private static final String TMP_DIR = "/home/hadoop/tmp";

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
            final int returnCode = copyProcess.run("/bin/bash", "-c", "cp -r * " + outputDir + "; rm -rf " + tmpDir.getPath());
            if (returnCode != 0) {
                throw new ProcessorException("execution of cp -r * " + outputDir + " failed: " + copyProcess.getOutputString());
            }
        } catch (InterruptedException e) {
            throw new ProcessorException("execution of cp -r * " + outputDir + " failed: " + e);
        }
    }
}

