package com.bc.calvados.hadoop.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.dataio.envisat.DSD;
import org.esa.beam.dataio.envisat.ProductFile;

import javax.imageio.stream.FileCacheImageInputStream;
import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class N1InputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    /**
     * Generate the list of files and make them into FileSplits.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        final List<InputSplit> inputSplits = super.getSplits(job);
        final List<InputSplit> n1Splits = new ArrayList<InputSplit>(inputSplits.size());
        Map<Path, Integer> headerSizeMap = new HashMap<Path, Integer>();
        Map<Path, Integer> granuleSizeMap = new HashMap<Path, Integer>();
        for (InputSplit inputSplit : inputSplits) {
            FileSplit split = (FileSplit) inputSplit;
            final Path file = split.getPath();
            int headerSize;
            int granuleSize;
            if (headerSizeMap.containsKey(file)) {
                headerSize = headerSizeMap.get(file);
                granuleSize = granuleSizeMap.get(file);
            }  else {
                FileSystem fs = file.getFileSystem(job.getConfiguration());
                FSDataInputStream fileIn = fs.open(file);
                ImageInputStream imageInputStream = new FileCacheImageInputStream(fileIn, new File("."));
                ProductFile productFile = ProductFile.open(imageInputStream);
                final org.esa.beam.dataio.envisat.RecordReader[] mdsRecordReaders = getMdsRecordReaders(productFile);
                headerSize = (int) mdsRecordReaders[0].getDSD().getDatasetOffset();
                granuleSize = computeGranuleSize(mdsRecordReaders);
                productFile.close();
                fileIn.close();
                headerSizeMap.put(file, headerSize);
                granuleSizeMap.put(file, granuleSize);
            }
            N1InputSplit n1Split = new N1InputSplit(split.getPath(), split.getStart(), split.getLength(), split.getLocations(), headerSize, granuleSize);
            n1Splits.add(n1Split);
        }
        return n1Splits;
    }

    static int computeGranuleSize(org.esa.beam.dataio.envisat.RecordReader[] mdsRecordReaders) {
        int granuleSize = 0;
        for (int i = 0; i < mdsRecordReaders.length; i++) {
            org.esa.beam.dataio.envisat.RecordReader recordReader = mdsRecordReaders[i];
            DSD dsd = recordReader.getDSD();
            granuleSize += dsd.getRecordSize();
        }
        return granuleSize;
    }

    static org.esa.beam.dataio.envisat.RecordReader[] getMdsRecordReaders(ProductFile productFile) throws IOException {
        String[] mdsNames = productFile.getValidDatasetNames('M');
        org.esa.beam.dataio.envisat.RecordReader[] recordReaders = new org.esa.beam.dataio.envisat.RecordReader[mdsNames.length];
        for (int i = 0; i < mdsNames.length; i++) {
            org.esa.beam.dataio.envisat.RecordReader recordReader = productFile.getRecordReader(mdsNames[i]);
            recordReaders[i] = recordReader;
        }
        return recordReaders;
    }

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new N1RecordReader();
    }

}
