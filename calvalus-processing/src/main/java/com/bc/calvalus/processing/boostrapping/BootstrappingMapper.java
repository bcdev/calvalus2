package com.bc.calvalus.processing.boostrapping;

import com.bc.calvalus.processing.executable.ExecutableProcessorAdapter;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A mapper that performs bootstrapping.
 *
 * @author MarcoZ
 * @author MarcoP
 */
public class BootstrappingMapper extends Mapper<NullWritable, NullWritable, IntWritable, Text> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        InputSplit inputSplit = context.getInputSplit();
        if (!(inputSplit instanceof NtimesInputSplit)) {
            throw new IllegalArgumentException("InputSplit is a " + inputSplit.getClass());
        }

        NtimesInputSplit ntimesInputSplit = (NtimesInputSplit) inputSplit;
        long numberOfIterations = ntimesInputSplit.getLength();
        Map<String, String> velocityProps = new HashMap<String, String>();
        velocityProps.put("numberOfIterations", Long.toString(numberOfIterations));

        ExecutableProcessorAdapter processorAdapter = new ExecutableProcessorAdapter(context);
        Path inputPath = new Path(conf.get(BootstrappingWorkflowItem.INPUT_FILE_PROPRTY));
        processorAdapter.copyFileToLocal(inputPath);
        File inputFile = processorAdapter.getInputFile();
        String[] outFiles = processorAdapter.processInput(ProgressMonitor.NULL,
                                                         null,
                                                         inputPath,
                                                         inputFile,
                                                         null,
                                                         velocityProps);

        if (outFiles != null && outFiles.length > 0) {
            String resultFile = outFiles[0];
            String result = FileUtils.readText(new File(processorAdapter.getCurrentWorkingDir(), resultFile));
            String[] headerAndBody = result.split("\n", 2);
            if (headerAndBody.length == 2) {
                context.write(new IntWritable(0), new Text(headerAndBody[0]+"\n"));
                context.write(new IntWritable(1), new Text(headerAndBody[1]));
            }
        }
    }
}
