package com.bc.calvalus.experiments.transfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;

/**
 * Tests the processing of an example MERIS L1 to a L2
 * using the HDFS on localhost for input and output.
 * Uses an input provided as resource with the test.
 * Requires that HDFS is running on localhost:9000 .
 * Verifies that the output directory exists in the end.
 *
 * @author Martin Boettcher
 */
public class HdfsTransferToolTest {
    private static final String TMP_INPUT = "/tmp/input";
    private static final String HDFS_OUTPUT = "hdfs://localhost:9000/output";

    @Test
    public void testTransferN1() throws Exception {

        // verify that input directory exists and is a directory
        final Path input = new Path(TMP_INPUT);
        FileSystem localFs = input.getFileSystem(new Configuration());
        assertTrue("input dir " + input, localFs.exists(input) && localFs.getFileStatus(input).isDir());

        // verify that output does not exist
        Path output = new Path(HDFS_OUTPUT);
        FileSystem hdfs = output.getFileSystem(new Configuration());
        assertTrue("output shall not exist: " + output, !hdfs.exists(output));

        // Run the map-reduce job
        ToolRunner.run(new TransferTool(),
                       new String[]{TMP_INPUT, HDFS_OUTPUT, "n1"});

        // verify that the output has been generated
        assertTrue("output shall now exist: " + output, hdfs.exists(output));
    }

    @Test
    public void testTransferLineInterleaved() throws Exception {

        // verify that input directory exists and is a directory
        final Path input = new Path(TMP_INPUT);
        FileSystem localFs = input.getFileSystem(new Configuration());
        assertTrue("input dir " + input, localFs.exists(input) && localFs.getFileStatus(input).isDir());

        // verify that output does not exist
        Path output = new Path(HDFS_OUTPUT);
        FileSystem hdfs = output.getFileSystem(new Configuration());
        assertTrue("output shall not exist: " + output, !hdfs.exists(output));

        // Run the map-reduce job
        ToolRunner.run(new TransferTool(),
                       new String[]{TMP_INPUT, HDFS_OUTPUT, "lineinterleaved"});

        // verify that the output has been generated
        assertTrue("output shall now exist: " + output, hdfs.exists(output));
    }

    @Test
    public void testTransferSliced() throws Exception {

        // verify that input directory exists and is a directory
        final Path input = new Path(TMP_INPUT);
        FileSystem localFs = input.getFileSystem(new Configuration());
        assertTrue("input dir " + input, localFs.exists(input) && localFs.getFileStatus(input).isDir());

        // verify that output does not exist
        Path output = new Path(HDFS_OUTPUT);
        FileSystem hdfs = output.getFileSystem(new Configuration());
        assertTrue("output shall not exist: " + output, !hdfs.exists(output));

        // Run the map-reduce job
        ToolRunner.run(new TransferTool(),
                       new String[]{TMP_INPUT, HDFS_OUTPUT, "sliced"});

        // verify that the output has been generated
        assertTrue("output shall now exist: " + output, hdfs.exists(output));
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

        FileSystem hdfs = new Path(HDFS_OUTPUT).getFileSystem(new Configuration());
        hdfs.delete(new Path(HDFS_OUTPUT), true);
    }

}
