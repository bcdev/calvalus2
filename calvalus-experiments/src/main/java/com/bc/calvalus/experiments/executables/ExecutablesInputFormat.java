package com.bc.calvalus.experiments.executables;

import com.bc.calvalus.experiments.processing.NoRecordReader;
import com.bc.calvalus.experiments.util.CalvalusLogger;
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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generates list of input splits for input files in job's request.
 *
 * @author Boe
 */
public class ExecutablesInputFormat extends InputFormat {

    private static final Logger LOG = CalvalusLogger.getLogger();

    //private static final String INPUTS_XPATH = "/wps:Execute/wps:DataInputs/wps:Input[ows:Identifier='calvalus.input']";
    //private static final String INPUT_HREF_XPATH = "wps:Reference/@xlink:href";
    private static final String INPUTS_XPATH = "/Execute/DataInputs/Input[Identifier='calvalus.input']";
    private static final String INPUT_HREF_XPATH = "Reference/@href";

    /**
     * Generate the list of files and make them into FileSplits.
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
            for (int i = 0; i < nodes.getLength(); ++i) {
                // get input out of request
                Node node = nodes.item(i);
                String inputUrl = request.getString(INPUT_HREF_XPATH, node);
                // inquire "status" of file from HDFS
                Path input = new Path(inputUrl);
                FileSystem fs = input.getFileSystem(job.getConfiguration());
                FileStatus[] file = fs.listStatus(input);
                if (file.length != 1) throw new FileNotFoundException(inputUrl + " not found");
                long length = file[0].getLen();
                BlockLocation[] locations = fs.getFileBlockLocations(file[0], 0, length);
                // create file split for the input
                FileSplit split = new FileSplit(input, 0, length, locations[0].getHosts());
                splits.add(split);
                LOG.info("split " + inputUrl);
            }
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
}
