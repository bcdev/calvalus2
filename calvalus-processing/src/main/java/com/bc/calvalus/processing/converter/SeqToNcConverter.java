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
public class SeqToNcConverter implements FormatConverter {

    private static final String TMP_DIR = "/home/hadoop/tmp";

    public void convert(String taskId, Path inputPath, String outputDir, String targetFormat, Configuration configuration)
        throws IOException
    {
        StreamingProductReader reader = new StreamingProductReader(inputPath, configuration);
        Product product = reader.readProductNodes(null, null);

        final File tmpDir = new File(TMP_DIR + File.separator + taskId);
        tmpDir.mkdirs();
        final String outputFilename = inputPath.getName().replace(".seq", ".nc");
        ProductIO.writeProduct(product, new File(tmpDir, outputFilename).getPath(), "NetCDF-BEAM");

        final ProcessUtil copyProcess = new ProcessUtil();
        copyProcess.directory(tmpDir);
        try {
            final int returnCode;
            if (outputDir.startsWith("hdfs:")) {
                returnCode = copyProcess.run("/bin/bash", "-c", "gzip -n " + outputFilename + "; hadoop fs -D dfs.replication=1 -copyFromLocal " + outputFilename + ".gz " + outputDir + "/" + outputFilename + ".gz; rm -rf " + tmpDir.getPath() + "; echo \"" + outputFilename + " copied to " + outputDir + "\"");
                System.out.println("cmdline input : " + "gzip -n " + outputFilename + "; hadoop fs -D dfs.replication=1 -copyFromLocal " + outputFilename + ".gz " + outputDir + "/" + outputFilename + ".gz; rm -rf " + tmpDir.getPath());
            } else {
                final String outputPath = (outputDir.startsWith("file:"))
                        ? new Path(outputDir).toUri().getPath()
                        : outputDir;
                returnCode = copyProcess.run("/bin/bash", "-c", "date; gzip -n *.nc; mkdir -p " + outputPath + "; cp -r * " + outputPath + "/; rm -rf " + tmpDir.getPath() + "; echo \"" + outputFilename + " copied to " + outputDir + "\"");
                System.out.println("cmdline input : " + "gzip -n *.nc; mkdir -p " + outputPath + "; cp -r * " + outputPath + "/; rm -rf " + tmpDir.getPath());
            }
            System.out.println("|" + copyProcess.getOutputString() + "|");
            if (returnCode != 0) {
                throw new ProcessorException("execution of cp -r * " + outputDir + " failed: " + copyProcess.getOutputString());
            }
        } catch (InterruptedException e) {
            throw new ProcessorException("execution of cp -r * " + outputDir + " failed: " + e);
        }
    }
}

