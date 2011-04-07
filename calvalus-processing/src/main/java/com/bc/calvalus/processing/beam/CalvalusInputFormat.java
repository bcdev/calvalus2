package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
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
import sun.security.krb5.internal.LoginOptions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generates list of input splits for input files in job's request.
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
                FileStatus[] file = fs.listStatus(input);
                if (file.length == 1) {
                    long length = file[0].getLen();
                    BlockLocation[] locations = fs.getFileBlockLocations(file[0], 0, length);
                    if (locations != null && locations.length != 0) {
                        // create file split for the input
                        FileSplit split = new FileSplit(input, 0, length, locations[0].getHosts());
                        splits.add(split);
                        LOG.info("split " + inputUrl);
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
