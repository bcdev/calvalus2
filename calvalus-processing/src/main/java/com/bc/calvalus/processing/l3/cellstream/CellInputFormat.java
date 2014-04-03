package com.bc.calvalus.processing.l3.cellstream;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.util.StringUtils;
import org.esa.beam.binning.operator.BinningConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for reading Cells (aka {@link com.bc.calvalus.processing.l3.L3TemporalBin TemporalBins})
 * from either {@link org.apache.hadoop.io.SequenceFile}s or from NetCDf/HDf files.
 */
public class CellInputFormat extends FileInputFormat<LongWritable, L3TemporalBin> {

    private static final String PART_FILE_PREFIX = "part-";

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> inputSplits = super.getSplits(job);
        List<InputSplit> cellSplits = new ArrayList<InputSplit>(inputSplits.size());
        for (InputSplit inputSplit : inputSplits) {
            FileSplit fileSplit = (FileSplit) inputSplit;
            cellSplits.add(new CellFileSplit(fileSplit));
        }
        return cellSplits;
    }

    @Override
    public RecordReader<LongWritable, L3TemporalBin> createRecordReader(InputSplit split,
                                                                        TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSplit fileSplit = (FileSplit) split;
        Path path = fileSplit.getPath();
        String filename = path.getName().toLowerCase();
        if (filename.startsWith(PART_FILE_PREFIX)) {
            return new SequenceFileRecordReader<LongWritable, L3TemporalBin>();
        } else {
            CellRecordReader reader = new CellRecordReader(path, conf);
            CellFileSplit cellFileSplit = (CellFileSplit) split;
            Map<String, String> metadata = cellFileSplit.getMetadata();
            metadata.put(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, StringUtils.arrayToString(reader.getFeatureNames()));
            metadata.put(JobConfigNames.CALVALUS_MIN_DATE, reader.getStartDate());
            metadata.put(JobConfigNames.CALVALUS_MAX_DATE, reader.getEndDate());

            BinningConfig binningConfig = new BinningConfig();
            binningConfig.setNumRows(reader.getNumRows());
            metadata.put(JobConfigNames.CALVALUS_L3_PARAMETERS, binningConfig.toXml());

            return reader;
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
        if (filename.startsWith(PART_FILE_PREFIX)) {
            return true;
        } else {
            return false;
        }
    }

    public Path getFirstInputDirectory(Job job) throws IOException {
        JobContext jobContext = new JobContext(job.getConfiguration(), null);
        List<FileStatus> fileStatuses = listStatus(jobContext);
        if (fileStatuses.isEmpty()) {
            return null;
        } else {
            return fileStatuses.get(0).getPath().getParent();
        }
    }
}
