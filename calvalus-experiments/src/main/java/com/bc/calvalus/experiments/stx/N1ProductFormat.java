package com.bc.calvalus.experiments.stx;

import com.bc.calvalus.hadoop.io.FSImageInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.dataio.envisat.DSD;
import org.esa.beam.dataio.envisat.ProductFile;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class N1ProductFormat extends FileInputFormat<IntWritable, IntWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        final List<InputSplit> inputSplits = super.getSplits(job);
        final List<InputSplit> n1Splits = new ArrayList<InputSplit>(inputSplits.size());
        Map<Path, Integer> headerSizeMap = new HashMap<Path, Integer>();
        Map<Path, Integer> granuleSizeMap = new HashMap<Path, Integer>();
        for (InputSplit inputSplit : inputSplits) {
            FileSplit split = (FileSplit) inputSplit;
            final Path path = split.getPath();
            int headerSize;
            int granuleSize;
            if (!headerSizeMap.containsKey(path)) {
                detectProdcutAnatomy(path, job, headerSizeMap, granuleSizeMap);
            }
            headerSize = headerSizeMap.get(path);
            granuleSize = granuleSizeMap.get(path);

            long splitStart = split.getStart();
            long splitEnd = splitStart + split.getLength();
            int yStart = 0;
            int height = 0;
            if (splitStart == 0) {
                splitStart = headerSize;
            } else {
                long pos = headerSize;
                while (pos <= splitStart) {
                    pos += granuleSize;
                    yStart++;
                }
                splitStart = pos;
            }
            long pos = splitStart;
            while (pos <= splitEnd) {
                pos += granuleSize;
                height++;
            }

            N1LineInputSplit n1Split = new N1LineInputSplit(split.getPath(), split.getStart(), split.getLength(), split.getLocations(), yStart, height);
            n1Splits.add(n1Split);
        }
        return n1Splits;
    }

    private void detectProdcutAnatomy(Path path, JobContext job, Map<Path, Integer> headerSizeMap, Map<Path, Integer> granuleSizeMap) throws IOException {
        ProductFile productFile = null;
        FSDataInputStream fileIn = null;
        try {
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            fileIn = fs.open(path);
            final FileStatus status = fs.getFileStatus(path);
            ImageInputStream imageInputStream = new FSImageInputStream(fileIn, status.getLen());
            productFile = ProductFile.open(imageInputStream);
            final org.esa.beam.dataio.envisat.RecordReader[] mdsRecordReaders = getMdsRecordReaders(productFile);
            int headerSize = (int) mdsRecordReaders[0].getDSD().getDatasetOffset();
            headerSizeMap.put(path, headerSize);
            int granuleSize = computeGranuleSize(mdsRecordReaders);
            granuleSizeMap.put(path, granuleSize);
        } finally {
            productFile.close();
            fileIn.close();
        }
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
    public RecordReader<IntWritable, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new N1LineNumberRecordReader();
    }


}
