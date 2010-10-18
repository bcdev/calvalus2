package com.bc.calvalus.experiments.processing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 
 */
public class LocalProcessingToolTest {
    private static final String TMP_INPUT = "/tmp/input";
    private static final String TMP_OUTPUT = "/tmp/output";

    @Test
    public void runProcessing() throws Exception {
        
        // verify that output is empty
        LocalFileSystem fs = LocalFileSystem.getLocal(new Configuration());
        Path outputFile = new Path(TMP_OUTPUT, "part-r-00000");
        assertTrue("Shall not exist: " + outputFile,
                   !fs.exists(outputFile));

        // provide input
        final String path = getClass().getResource("/MER_RR__1P.N1").getPath();
        fs.copyFromLocalFile(new Path(path), new Path(TMP_INPUT, "MER_RR__1P.N1"));
        assertTrue(fs.exists(new Path(TMP_INPUT, "MER_RR__1P.N1")));

        // Run the map-reduce job
        ToolRunner.run(new ProcessingTool(), new String[] {
                TMP_INPUT, TMP_OUTPUT
        });

        assertTrue("Shall now exist: " + outputFile,
                   fs.exists(outputFile));
    }

    @Before
    public void start() throws IOException {
        clearWorkingDirs();
    }

//    @After
//    public void stop() throws IOException {
//        clearWorkingDirs();
//    }

    private void clearWorkingDirs() throws IOException {
        LocalFileSystem fs = LocalFileSystem.getLocal(new Configuration());
        fs.delete(new Path(TMP_INPUT), true);
        fs.delete(new Path(TMP_OUTPUT), true);
    }

}
