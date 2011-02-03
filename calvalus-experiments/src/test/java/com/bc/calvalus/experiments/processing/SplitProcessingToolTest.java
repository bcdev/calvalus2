package com.bc.calvalus.experiments.processing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.junit.Assert.assertEquals;
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
@Ignore
public class SplitProcessingToolTest {
    private static final String TMP_INPUT = "hdfs://localhost:9000/user/boe/meris-l1";
    private static final String TMP_OUTPUT = "hdfs://localhost:9000/user/boe/meris-l2";
    private static final String INPUT_FILE = "MER_RR__1PQACR20040526_091235_000026432027_00122_11699_0000.N1";

    private static final long TEST_SPLIT_SIZE = 17 * 1024*1024;

    /**
     * Verifies that splits are generated properly.
     * <ul>
     * <li>The splits cover the product continuously and completely.</li>
     * <li>The split borders match record borders.</li>
     * <li>The splits do not cross block borders arbitrarily.</li>
     * <li>The size of splits are close to the desired one except for splits at end of blocks.</li>
     * </ul>
     * Prerequisite: Local Hadoop is up and line-interleaved input is in HDFS file system
     */
    @Test
    public void testSplit() throws Exception {

        // verify that input exists
        final Path input = new Path(TMP_INPUT, INPUT_FILE);
        FileSystem inputFileSystem = input.getFileSystem(new Configuration());
        assertTrue(inputFileSystem.exists(new Path(TMP_INPUT, INPUT_FILE)));

        // construct job and set parameters and handlers
        Configuration conf = new Configuration();
        final Job job = new Job(conf, "L2SplitTest");
        job.getConfiguration().setLong(N1InterleavedInputFormat.SPLIT_SIZE_PARAM, TEST_SPLIT_SIZE);

        // split it using a N1 product input format
        FileInputFormat.addInputPath(job, input);
        final N1InterleavedInputFormat format = new N1InterleavedInputFormat();
        final List<InputSplit> splits = format.getSplits(job);

        FileStatus fileStatus = inputFileSystem.getFileStatus(input);
        BlockLocation[] blocks = inputFileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        assertTrue("at least one split?", splits.size() > 0);
        N1InterleavedInputSplit firstSplit = (N1InterleavedInputSplit) splits.get(0);
        long headerSize = firstSplit.getStart();
        long granuleSize = firstSplit.getLength() / firstSplit.getNumberOfRecords();

        // check whether splits cover product continuously
        checkCovering(splits, fileStatus.getLen());

        // check whether split borders match record borders
        checkMatchRecords(splits, headerSize, granuleSize);

        // check whether splits do not cross block borders arbitrarily
        checkMatchBlocks(splits, blocks, granuleSize);

        // check whether size of splits are close to the desired one except for splits at end of blocks
        checkSize(splits, blocks, granuleSize, TEST_SPLIT_SIZE);
    }

    /**
     * Checks whether splits cover product continuously
     */
    private void checkCovering(List<InputSplit> splits, long productLength) {
        long cursor = 0;
        for (InputSplit s : splits) {
            N1InterleavedInputSplit split = (N1InterleavedInputSplit) s;
            if (cursor == 0) {
                assertTrue("header?", split.getStart() > 0);
            } else {
                assertEquals("continuous?", cursor, split.getStart());
            }
            assertTrue("not empty?", split.getNumberOfRecords() > 0);
            cursor = split.getStart() + split.getLength();
        }
        assertEquals("covering?", cursor, productLength);
    }

    /**
     * Checks whether split borders match record borders
     */
    private void checkMatchRecords(List<InputSplit> splits, long headerSize, long granuleSize) {
        for (InputSplit s : splits) {
            N1InterleavedInputSplit split = (N1InterleavedInputSplit) s;
            long end = split.getStart() + split.getLength();
            assertTrue("match record border?", (end - headerSize) % granuleSize == 0);
        }
    }

    /**
     * Checks whether splits do not cross block borders arbitrarily
     */
    private void checkMatchBlocks(List<InputSplit> splits, BlockLocation[] block, long granuleSize) {
        int currentBlock = 0;
        for (InputSplit s : splits) {
            N1InterleavedInputSplit split = (N1InterleavedInputSplit) s;
            long end = split.getStart() + split.getLength();
            // split touches or crosses block border?
            if (end > block[currentBlock].getOffset() + block[currentBlock].getLength()) {
                assertTrue("match block?",
                           split.getStart() - (block[currentBlock].getOffset() + block[currentBlock].getLength()) < granuleSize);
                ++currentBlock;
            }
        }
    }

    /**
     * Checks whether size of splits are close to the desired one except for splits at end of blocks
     */
    private void checkSize(List<InputSplit> splits, BlockLocation[] block, long granuleSize, long testSplitSize) {
        int currentBlock = 0;
        for (InputSplit s : splits) {
            N1InterleavedInputSplit split = (N1InterleavedInputSplit) s;
            long end = split.getStart() + split.getLength();
            // split touches or crosses block border?
            if (end > block[currentBlock].getOffset() + block[currentBlock].getLength() - granuleSize) {
                ++currentBlock;
                assertTrue("split size?", split.getLength() >= testSplitSize / 2 || split.getStart() == 0);
            } else {
                assertTrue("split size?", Math.abs(split.getLength() - testSplitSize) < granuleSize);
            }
        }
    }

    /**
     * Verifies that processing in splits executes and generates some output.
     * <p/>
     * Prerequisite: Local Hadoop is up and line-interleaved input is in HDFS file system
     */
    @Test
    public void runProcessing() throws Exception {

        // verify that output is empty
        Path output = new Path(TMP_OUTPUT);
        FileSystem outputFileSystem = output.getFileSystem(new Configuration());
        assertTrue("Shall not exist: " + output,
                   ! outputFileSystem.exists(output));

        // verify that input exists
        final Path input = new Path(TMP_INPUT, INPUT_FILE);
        FileSystem inputFileSystem = input.getFileSystem(new Configuration());
        assertTrue(inputFileSystem.exists(new Path(TMP_INPUT, INPUT_FILE)));

        // Run the map-reduce job
        ToolRunner.run(new L2ProcessingTool(),
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
