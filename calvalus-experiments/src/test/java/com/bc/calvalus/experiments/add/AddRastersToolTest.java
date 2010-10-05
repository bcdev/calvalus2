package com.bc.calvalus.experiments.add;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AddRastersToolTest {
    private static final String TMP_INPUT = "/tmp/input";
    private static final String TMP_OUTPUT = "/tmp/output";

    @Test
    public void runAddRasters() throws Exception {
        LocalFileSystem fs = LocalFileSystem.getLocal(new Configuration());
        Path outputFile = new Path(TMP_OUTPUT, "part-r-00000");

        assertTrue("Shall not exist: " + outputFile,
                   !fs.exists(outputFile));

        // Run the map-reduce job
        ToolRunner.run(new AddRastersTool(), new String[]{
                TMP_INPUT, TMP_OUTPUT
        });

        assertTrue("Shall now exist: " + outputFile,
                   fs.exists(outputFile));

        long outputLength = fs.getFileStatus(outputFile).getLen();
        assertEquals(10 * 10, outputLength);

        FSDataInputStream isOutput = fs.open(outputFile);
        for (int i = 0; i < outputLength; i++) {
            byte expected = (byte) (AddRastersTool.generateInputRasterPixel(0, i)
                    + AddRastersTool.generateInputRasterPixel(1, i)
                    + AddRastersTool.generateInputRasterPixel(2, i));
            assertEquals("Invalid output pixel " + i,
                         expected, (byte) isOutput.read());
        }
    }

    @Before
    public void start() throws IOException {
        clearWorkingDirs();
    }

    @After
    public void stop() throws IOException {
        clearWorkingDirs();
    }

    private void clearWorkingDirs() throws IOException {
        LocalFileSystem fs = LocalFileSystem.getLocal(new Configuration());
        fs.delete(new Path(TMP_INPUT), true);
        fs.delete(new Path(TMP_OUTPUT), true);
    }

}
