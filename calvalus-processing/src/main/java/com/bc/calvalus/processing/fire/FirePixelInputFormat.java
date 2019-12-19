package com.bc.calvalus.processing.fire;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * @author thomas
 */
public class FirePixelInputFormat extends InputFormat {

    private final PatternBasedInputFormat delegate;

    public FirePixelInputFormat() {
        delegate = new PatternBasedInputFormat();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        ContinentalArea area = ContinentalArea.valueOf(conf.get("calvalus.area"));

        String year = conf.get("calvalus.year");
        String month = conf.get("calvalus.month");
        String inputRootDir = conf.get("calvalus.inputRootDir"); //  hdfs://calvalus//calvalus/projects/c3s/olci-ba-v1.7.11

        String tiles = getTiles(area);
        String tilesSpec = "(" + tiles + ")";

        // looks slightly weird, but 0 splits are found if tilesSpec is put into directory instead of filename
        String inputPathPattern = String.format(inputRootDir + "/.*/%s-%s/.*outputs-%s.*gz", year, month, tilesSpec);

        conf.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, inputPathPattern);
        return delegate.getSplits(context);
    }

    private String getTiles(ContinentalArea area) {
        Properties tiles = new Properties();
        try {
            tiles.load(getClass().getResourceAsStream("areas-tiles-olci.properties"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return tiles.getProperty(area.name());
    }

    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return delegate.createRecordReader(split, context);
    }

}
