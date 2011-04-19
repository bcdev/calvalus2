package com.bc.calvalus.processing;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An input format that maps each input file to a single (file) split.
 * Input files are given by the configuration parameter with the name
 * {@link JobConfNames#CALVALUS_INPUT}. Its value is expected to
 * be a comma-separated list of file paths (HDFS URLs).
 * <p/>
 * <b>Imporatnt Note:</b> The implementation
 *
 * @author Boe
 */
public class CalvalusInputFormat extends InputFormat {

    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Generate the list of files and make them into FileSplits.
     */
    @Override
    public List<FileSplit> getSplits(JobContext job) throws IOException {

        try {
            // parse request
            Configuration configuration = job.getConfiguration();
            String[] requestInputPaths = configuration.get(JobConfNames.CALVALUS_INPUT).split(",");

            // create splits for each calvalus.input in request
            List<FileSplit> splits = new ArrayList<FileSplit>(requestInputPaths.length);
            for (String inputUrl : requestInputPaths) {
                // get input out of request
                // inquire "status" of file from HDFS
                Path input = new Path(inputUrl);
                FileSystem fs = input.getFileSystem(configuration);
                FileStatus[] fileStatuses = fs.listStatus(input);
                if (fileStatuses.length == 1) {
                    FileStatus fileStatus = fileStatuses[0];
                    long length = fileStatus.getLen();
                    BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, length);
                    if (locations != null && locations.length != 0) {
                        // create file split for the input
                        FileSplit split = new FileSplit(input, 0, length, locations[0].getHosts());
                        splits.add(split);
                    } else {
                        LOG.warning("failed to fs.getFileBlockLocations for: " + inputUrl);
                    }
                } else {
                    LOG.warning(inputUrl + " not found");
                }
            }
            return splits;
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "failed to split input set: " + e.toString(), e);
            throw e;
        }
    }

    /**
     * Create a record reader for a given split. Not used here.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
