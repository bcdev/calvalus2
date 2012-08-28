package com.bc.calvalus.processing.shellexec;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
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
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Generates list of input splits for input files in job's request.
 *
 * @author Boe
 */
@Deprecated
public class ExecutablesInputFormat extends InputFormat {

    private static final Logger LOG = CalvalusLogger.getLogger();

    //private static final String INPUTS_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.input']";
    //private static final String INPUT_HREF_XPATH = "wps:Reference/@xlink:href";
    private static final String INPUT_PATTERN_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.filenamepattern']/Data/LiteralData";
    private static final String INPUTS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.input']";
    private static final String INPUT_HREF_XPATH = "Reference/@href";

    /**
     * Generates FileSplits for inputs, searches directory tree if directories are given as calvalus inputs.
     */
    @Override
    public List<FileSplit> getSplits(JobContext job) throws IOException {

        try {

            // parse request
            final String requestContent = job.getConfiguration().get("calvalus.request");
            final XmlDoc request = new XmlDoc(requestContent);
            NodeList nodes = request.getNodes(INPUTS_XPATH);

            // create splits for each calvalus.input in request
            List<FileSplit> splits = new ArrayList<FileSplit>(nodes.getLength());
            String pattern = request.getString(INPUT_PATTERN_XPATH, (String) null);
            Pattern filter = (pattern != null) ? Pattern.compile(pattern) : null;
            for (int i = 0; i < nodes.getLength(); ++i) {
                // get input out of request
                Node node = nodes.item(i);
                String inputUrl = request.getString(INPUT_HREF_XPATH, node);
                // inquire "status" of file from HDFS
                Path input = new Path(inputUrl);
                FileSystem fs = input.getFileSystem(job.getConfiguration());
                FileStatus[] files = fs.listStatus(input);
                collectInputFiles(files, fs, filter, splits);
            }
            if (splits.size() == 0) throw new FileNotFoundException("no file(s) found in input path(s)");

            // largest first ...
            Collections.sort(splits, new Comparator<FileSplit>() {
                @Override
                public int compare(FileSplit o1, FileSplit o2) {
                    return o1.getLength() > o2.getLength() ? -1 : o1.getLength() < o2.getLength() ? 1 : 0;
                }
            });

            return splits;

        } catch (IOException e) {

            LOG.log(Level.SEVERE, "failed to split input set: " + e.toString(), e);
            throw e;

        } catch (Exception e) {

            LOG.log(Level.SEVERE, "failed to split input set: " + e.toString(), e);
            throw new IOException("failed to split input set: " + e.toString(), e);

        }
    }

    /**
     * Create a record reader for a given split. Not used here.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }

    /**
     * Recursively finds files in directory tree and creates a split for each
     * of them. Optionally applies filter to file name.
     * @param files  array of files of a directory, or a single file (if file is specified in calvalus input parameter)
     * @param fs     file system to be used for directory listing
     * @param filter pattern to be applied to file name, null if no filter is defined
     * @param accu   accumulator for FileSplit entries for files found
     * @throws IOException  if list directory fails
     */
    static void collectInputFiles(FileStatus[] files, FileSystem fs, Pattern filter, List<FileSplit> accu) throws IOException {
        for (FileStatus file : files) {
            final Path path = file.getPath();
            if (file.isDir()) {
                FileStatus[] subdir = fs.listStatus(path);
                collectInputFiles(subdir, fs, filter, accu);
            } else {
                if (filter == null || filter.matcher(path.getName()).matches()) {
                    long length = file.getLen();
                    BlockLocation[] locations = fs.getFileBlockLocations(file, 0, length);
                    if (locations == null || locations.length == 0) {
                        LOG.warning("failed to fs.getFileBlockLocations for: " + path);
                    } else {
                        // create file split for the input
                        FileSplit split = new FileSplit(path, 0, length, locations[0].getHosts());
                        accu.add(split);
                        LOG.info("split " + path);
                    }
                }
            }
        }
    }

}
