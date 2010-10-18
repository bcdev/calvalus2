package com.bc.calvalus.experiments.processing;

import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Generator of splits for case E "interleaved" based on desired split size
 * which should be a factor of the block size. Assumes without checking that
 * the product is consistent, i.e. the sum of the block sizes of a product
 * are the header size plus a multiple of the granule sizes of that product.
 * <p/>
 *
 * Simplified pseudo code:
 * <pre>
 *   For each input file
 *     repeatedly determine next split position
 *     optionally adjust it to (record border before) block border
 *     and generate splits for all of them
 * </pre>
 *
 * Desired split size in Bytes is provided as parameter splits.desiredSizeInBytes
 */
public class N1InterleavedInputFormat extends FileInputFormat {

    public static final String SPLIT_SIZE_PARAM = "job.splitSize";
    private static final long DEFAULT_SPLIT_SIZE_IN_BYTES = 64 * 1024 * 1024;

    private static final Logger LOG = CalvalusLogger.getLogger();

    public List<InputSplit> getSplits(JobContext job) throws IOException {

        // determine algorithm parameters
        final List<InputSplit> splitAccu = new ArrayList<InputSplit>();
        final List<InputSplit> blocks = super.getSplits(job);
        final long desiredSplitSizeInBytes =
                job.getConfiguration().getLong(SPLIT_SIZE_PARAM, DEFAULT_SPLIT_SIZE_IN_BYTES);
        //LOG.config("desired split size [Bytes]: " + desiredSplitSizeInBytes);
        LOG.info("desired split size [Bytes]: " + desiredSplitSizeInBytes);

        // introduce variables for split borders
        Path             prevBlockPath = null;
        N1ProductAnatomy productAnatomy = null;
        long startOfSplit = 0;  // byte position within product of first record of split
        long endOfSplit = 0;  // computed and optionally adjusted byte position of end of last record of split
        int numberOfRecords = 0;  // number of records in split

        // loop over blocks
        for (InputSplit b : blocks) {
            FileSplit block = (FileSplit) b;
            LOG.finer("next block to be split: " + block);
            // determine header size and record size of new file
            if (! block.getPath().equals(prevBlockPath)) {
                productAnatomy = new N1ProductAnatomy(block.getPath(), job);
                LOG.finer("anatomy of next product: " + productAnatomy);
                prevBlockPath = block.getPath();
                startOfSplit = productAnatomy.getHeaderSize();
                numberOfRecords = (int) ((desiredSplitSizeInBytes + productAnatomy.getGranuleSize() - 1)
                                / productAnatomy.getGranuleSize());
            } else {
                assert productAnatomy != null;
            }

            // loop over splits to be generated within block
            while (startOfSplit + productAnatomy.getGranuleSize() <= block.getStart() + block.getLength()) {
                endOfSplit = startOfSplit + numberOfRecords * productAnatomy.getGranuleSize();
                // adjust end if close to or beyond end of block
                if (endOfSplit > block.getStart() + block.getLength() - numberOfRecords * productAnatomy.getGranuleSize() / 2) {
                    endOfSplit = startOfSplit
                                 + ((block.getStart() + block.getLength() - startOfSplit)
                                    / productAnatomy.getGranuleSize())
                                   * productAnatomy.getGranuleSize();
                    LOG.finer("adjusting end of split to " + endOfSplit + " to match block "
                              + (block.getStart() + block.getLength()));
                }
                // create split and add to accumulator
                N1InterleavedInputSplit split =
                        new N1InterleavedInputSplit(block.getPath(),
                                                    startOfSplit,
                                                    endOfSplit-startOfSplit,
                                                    block.getLocations(),
                                                    /* startRecord */ (int) ((startOfSplit - productAnatomy.getHeaderSize()) / productAnatomy.getGranuleSize()),
                                                    /* numberOfRecords */ (int) ((endOfSplit - startOfSplit) / productAnatomy.getGranuleSize()));
                //LOG.fine("split generated: " + split);
                LOG.info("split generated: " + split);
                //System.out.println("split generated: " + split);
                splitAccu.add(split);
                startOfSplit = endOfSplit;
            }
        }

        return splitAccu;
    }


    /**
     * Formally creation of empty record reader for a split.
     * Not used to read records as the complete split is provided to BEAM as subset.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RecordReader<NullWritable, NullWritable> () {

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                //intentionally empty
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }

            @Override
            public NullWritable getCurrentKey() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public NullWritable getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                //intentionally empty
            }
        };
    }
}
