package com.bc.calvalus.processing.boostrapping;

import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.bc.calvalus.processing.boostrapping.BootstrappingWorkflowItem.*;

/**
 * An input format that creates N input splits randomly distributed.
 *
 * @author MarcoZ
 * @author MarcoP
 */
public class NtimesInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numberOfIterations = conf.getInt(NUM_ITERATIONS_PROPERTY, NUM_ITERATIONS_DEFAULT);
        int iterationsPerNode = conf.getInt(ITERATION_PER_NODE_PROPERTY, ITERATION_PER_NODE_DEFAULT);
        int numberOfSplits = numberOfIterations / iterationsPerNode;
        int iterationsRemaining = numberOfIterations % iterationsPerNode;

        List<InputSplit> splits = new ArrayList<InputSplit>(numberOfSplits + 1);
        for (int i = 0; i < numberOfSplits; i++) {
            splits.add(new NtimesInputSplit(iterationsPerNode));
        }
        splits.add(new NtimesInputSplit(iterationsRemaining));
        return splits;
    }


    /**
     * Creates a {@link com.bc.calvalus.processing.hadoop.NoRecordReader} because records are not used with this input format.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
                                                                       TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new NoRecordReader();
    }
}
