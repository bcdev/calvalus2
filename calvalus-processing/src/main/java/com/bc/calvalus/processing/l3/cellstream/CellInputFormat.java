package com.bc.calvalus.processing.l3.cellstream;

import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.IOException;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for reading Cells (aka {@link com.bc.calvalus.processing.l3.L3TemporalBin TemporalBins})
 * from either {@link org.apache.hadoop.io.SequenceFile}s or from NetCDf/HDf files.
 */
public class CellInputFormat extends FileInputFormat<LongWritable, L3TemporalBin> {

    private static final String SUFFIX_NETCDF = ".nc";
    private static final String SUFFIX_HDF = ".hdf";

    @Override
    public RecordReader<LongWritable, L3TemporalBin> createRecordReader(InputSplit split,
                                                                        TaskAttemptContext context) throws IOException {
        Configuration configuration = context.getConfiguration();
        FileSplit fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();
        String filename = path.getName().toLowerCase();
        if (filename.endsWith(SUFFIX_NETCDF) || filename.endsWith(SUFFIX_HDF)) {
            CellRecordReader reader = new CellRecordReader();
            configuration.setStrings("calvalus.l3.inputFeatureNames", reader.getFeatureNames());
            return reader;
        } else {
            FileSystem fs = path.getFileSystem(configuration);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, configuration);
            SequenceFile.Metadata metadata = reader.getMetadata();
            Text featureNamesText = metadata.get(new Text("calvalus.l3.inputFeatureNames"));
            if (featureNamesText != null) {
                configuration.setStrings("calvalus.l3.inputFeatureNames", featureNamesText.toString());
            }
            reader.close();
            return new SequenceFileRecordReader<LongWritable, L3TemporalBin>();
        }
    }

    /**
     * Only {@link org.apache.hadoop.io.SequenceFile}s will be splitted.
     */
    @Override
    protected long getFormatMinSplitSize() {
        return SequenceFile.SYNC_INTERVAL;
    }

    /**
     * Only {@link org.apache.hadoop.io.SequenceFile}s will be splitted.
     */
    @Override
    protected boolean isSplitable(JobContext context, Path path) {
        String filename = path.getName().toLowerCase();
        if (filename.endsWith(SUFFIX_NETCDF) || filename.endsWith(SUFFIX_HDF)) {
            return false;
        } else {
            return true;
        }
    }
}
