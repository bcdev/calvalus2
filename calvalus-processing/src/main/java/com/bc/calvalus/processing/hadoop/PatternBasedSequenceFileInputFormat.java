package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;
import java.util.List;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class PatternBasedSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {
    private PatternBasedInputFormat delegate = new PatternBasedInputFormat();

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        return delegate.getSplits(job);
    }
}
