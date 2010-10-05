package com.bc.calvalus.experiments.add;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AddRastersToolTest {
    private static final String TMP_INPUT = "/tmp/input";
    private static final String TMP_OUTPUT = "/tmp/output";

    @Test
    public void runAddRasters() throws Exception {
        LocalFileSystem fileSystem = LocalFileSystem.getLocal(new Configuration());
        Path outputFile = new Path(TMP_OUTPUT, "part-r-00000");

        assertTrue("Shall not exist: " + outputFile,
                   !fileSystem.exists(outputFile));

        // Run the map-reduce job
        ToolRunner.run(new AddRastersTool(), new String[]{
                TMP_INPUT, TMP_OUTPUT
        });

        assertTrue("Shall now exist: " + outputFile,
                   fileSystem.exists(outputFile));

        FSDataInputStream isOutput = fileSystem.open(outputFile);
        for (int i = 0; i < 4; i++) {
            assertEquals("Invalid output pixel " + i,
                         AddRastersTool.generateInputRasterPixel(0, i)
                                 + AddRastersTool.generateInputRasterPixel(1, i)
                                 + AddRastersTool.generateInputRasterPixel(2, i),
                         isOutput.read());
        }

        fileSystem.delete(new Path(TMP_INPUT), true);
        fileSystem.delete(new Path(TMP_OUTPUT), true);
    }

}
