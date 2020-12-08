package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * An input format that maps each entry in a table to a single (file) split, optionally with parameters.
 * <p/>
 * Input files are given by the configuration parameter
 * {@link JobConfigNames#CALVALUS_INPUT_TABLE CALVALUS_INPUT_TABLE}.
 * Its value is expected to be a path to a table of file paths in first column and parameters in additional columns.
 * The heading row provides the names of the processing parameters.
 *
 * @author Martin
 */
public class TableInputFormat extends InputFormat {

    protected static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Maps each input file to a single (file) split.
     * <p/>
     * Input files are given by the configuration parameter
     * {@link JobConfigNames#CALVALUS_INPUT_PATH_PATTERNS}. Its value is expected to
     * be a comma-separated list of file path patterns (HDFS URLs). These patterns can contain dates and region names.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        // parse request
        Configuration configuration = job.getConfiguration();
        final Path inputTablePath = new Path(configuration.get(JobConfigNames.CALVALUS_INPUT_TABLE));
        final FileSystem tableFS = inputTablePath.getFileSystem(configuration);
        final BufferedReader in = new BufferedReader(new InputStreamReader(tableFS.open(inputTablePath)));
        String headLine;
        do {
            headLine = in.readLine();
            if (headLine == null) {
                throw new IOException("no headline found in " + inputTablePath.getName());
            }
        } while (headLine.startsWith("#"));
        final String[] attributes = headLine.split("[ \t]+");
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int fileCounter = 0;
        boolean irregularTableDetected = false;
        while (true) {
            final String line = in.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("#")) {
                continue;
            }
            final String[] values = line.split("\t");
            if (! irregularTableDetected && values.length != attributes.length) {
                LOG.warning("values " + (values.length - 1) + " does not match attributes " +
                                    (attributes.length - 1) + " for " + values[0]);
                irregularTableDetected = true;
            }
            ++fileCounter;
            int inputIndex = attributes.length > 0 && "output".equals(attributes[0]) ? 1 : 0;
            // collect parameters
            final String[] parameters = new String[values.length * 2 - 2];
            int j = 0;
            for (int i = 0; i < values.length; ++i) {
                if (i != inputIndex) {
                    parameters[j++] = i < attributes.length ? attributes[i] : "parameter";
                    parameters[j++] = values[i];
                }
            }
            // determine path, length, hosts
            final Path path = new Path(values[inputIndex]);
            final long size;
            final String[] hosts;
            if (values[inputIndex].startsWith("s3a:")) {
                // check access to file on server side only
                size = -1;
                hosts = null;
            } else {
                // check access etc. on client side and skip missing and empty files
                final FileSystem fileSystem = path.getFileSystem(configuration);
                FileStatus status;
                try {
                    status = fileSystem.getFileStatus(path);
                } catch (FileNotFoundException e) {
                    status = null;
                }
                if (status == null) {
                    LOG.warning("cannot find input " + values[inputIndex] + ". skipping");
                    continue;
                }
                BlockLocation[] locations = fileSystem.getFileBlockLocations(status, 0, status.getLen());
                if (locations == null || locations.length == 0) {
                    LOG.warning("cannot find hosts of input " + values[inputIndex] + ". skipping");
                    continue;
                }
                size = status.getLen();
                hosts = locations[0].getHosts();
            }
            // add parameterized split and count
            splits.add(new ParameterizedSplit(path, size, hosts, parameters));
        }
        if (splits.size() == 0) {
            throw new IOException("no splits found in table " + inputTablePath.getName());
        }
        LOG.info(splits.size() + " splits added from table " + inputTablePath.getName() + " (from " + fileCounter + " listed).");
        return splits;
    }

    /**
     * Creates a {@link NoRecordReader} because records are not used with this input format.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
                                                                       TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new NoRecordReader();
    }
}
