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
 * Tests the processing of an example MERIS L1 to a L2
 * using the HDFS on localhost for input and output.
 * Uses an input provided as resource with the test.
 * Requires that HDFS is running on localhost:9000 .
 */
public class HdfsProcessingToolTest {
    private static final String TMP_INPUT = "hdfs://localhost:9000/input";
    private static final String TMP_OUTPUT = "hdfs://localhost:9000/output";

    @Test
    public void runProcessing() throws Exception {

        // verify that output is empty
        Path output = new Path(TMP_OUTPUT, "part-r-00000");
        FileSystem outputFileSystem = output.getFileSystem(new Configuration());
        assertTrue("Shall not exist: " + output,
                   ! outputFileSystem.exists(output));

        // copy input from resource to input directory
        final String inputResource = getClass().getResource("/MER_RR__1P.N1").getPath();
        final Path input = new Path(TMP_INPUT, "MER_RR__1P.N1");
        FileSystem inputFileSystem = input.getFileSystem(new Configuration());
        inputFileSystem.copyFromLocalFile(new Path(inputResource), input);
        assertTrue(inputFileSystem.exists(new Path(TMP_INPUT, "MER_RR__1P.N1")));

        // Run the map-reduce job
        ToolRunner.run(new ProcessingTool(),
                       new String[] { TMP_INPUT, TMP_OUTPUT});

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
        FileSystem inputFileSystem = fileSystemOf(TMP_INPUT);
        FileSystem outputFileSystem = fileSystemOf(TMP_OUTPUT);
        // delete input dir and output dir
        inputFileSystem.delete(new Path(TMP_INPUT), true);
        outputFileSystem.delete(new Path(TMP_OUTPUT), true);
    }

    private static FileSystem fileSystemOf(String uriString) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        URI uri = new URI(uriString);
        if ("hdfs".equals(uri.getScheme())) {
            return FileSystem.get(uri, conf);
        } else {
            return LocalFileSystem.getLocal(conf);
        }
    }
}
