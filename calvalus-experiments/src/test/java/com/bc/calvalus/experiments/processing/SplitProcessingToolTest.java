package com.bc.calvalus.experiments.processing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;

/**
 * Tests the concurrent processing of an example MERIS L1 to a L2
 * using the HDFS on localhost for input and output.
 * Uses an input provided in the HDFS before the test.
 * Requires that HDFS is running on localhost:9000 .
 * Verifies that the output directory exists in the end. 
 *
 * @author Martin Boettcher
 */
public class SplitProcessingToolTest {
    //private static final String TMP_INPUT = "hdfs://localhost:9000/user/boe/meris-l1";
    //private static final String TMP_OUTPUT = "hdfs://localhost:9000/user/boe/meris-l2";
    private static final String TMP_INPUT = "/home/boe/samples";
    private static final String TMP_OUTPUT = "/home/boe/feasibility/hadoop/output6";
    private static final String INPUT_FILE = "MER_RR__1PQACR20040526_091235_000026432027_00122_11699_0000.N1";

    @Test
    public void runProcessing() throws Exception {

        // verify that output is empty
        Path output = new Path(TMP_OUTPUT, "map-of-slices.txt");
        FileSystem outputFileSystem = output.getFileSystem(new Configuration());
        assertTrue("Shall not exist: " + output,
                   ! outputFileSystem.exists(output));

        // verify that input exists
        final Path input = new Path(TMP_INPUT, INPUT_FILE);
        FileSystem inputFileSystem = input.getFileSystem(new Configuration());
        assertTrue(inputFileSystem.exists(new Path(TMP_INPUT, INPUT_FILE)));

        // Run the map-reduce job
        ToolRunner.run(new ProcessingTool(),
                       new String[] { TMP_INPUT, TMP_OUTPUT, "-lineInterleaved" , "-splitSize=16777216" });

        // verify that the output has been generated
        assertTrue("Shall now exist: " + output,
                   outputFileSystem.exists(output));
    }

    @Before
    public void start() throws IOException, URISyntaxException {
        clearWorkingDirs();
    }

//    @After
//    public void stop() throws IOException {
//        clearWorkingDirs();
//    }

    private void clearWorkingDirs() throws IOException, URISyntaxException {  
        // determine input file system and output file system
        //FileSystem inputFileSystem = fileSystemOf(TMP_INPUT);
        FileSystem outputFileSystem = fileSystemOf(TMP_OUTPUT);
        // delete input dir and output dir
        //inputFileSystem.delete(new Path(TMP_INPUT), true);
        outputFileSystem.delete(new Path(TMP_OUTPUT), true);
    }

    private static FileSystem fileSystemOf(String uriString) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        Path path = new Path(uriString);
        return path.getFileSystem(conf);
    }
}
