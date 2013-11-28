package com.bc.calvalus.processing.boostrapping;

import com.bc.calvalus.processing.executable.ExecutableProcessorAdapter;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.util.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * A mapper that performs bootstrapping.
 *
 * @author MarcoZ
 * @author MarcoP
 */
public class BootstrappingMapper extends Mapper<NullWritable, NullWritable, Text, Text> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        InputSplit inputSplit = context.getInputSplit();
        if (!(inputSplit instanceof NtimesInputSplit)) {
            throw new IllegalArgumentException("InputSplit is a " + inputSplit.getClass());
        }

        NtimesInputSplit ntimesInputSplit = (NtimesInputSplit) inputSplit;
        long numberOfIterations = ntimesInputSplit.getLength();
        // TODO feed number of iteration into the script

        ExecutableProcessorAdapter processorAdapter = new ExecutableProcessorAdapter(context);
        Path inputPath = new Path(conf.get(BootstrappingWorkflowItem.INPUT_FILE_PROPRTY));
        File inputFile = processorAdapter.copyFileToLocal(inputPath);
        String[] outFiles = processorAdapter.processInput(ProgressMonitor.NULL,
                                                         null,
                                                         inputPath,
                                                         inputFile);
        if (outFiles != null && outFiles.length > 0) {
            String result = FileUtils.readText(new File(processorAdapter.getCurrentWorkingDir(), outFiles[0]));
            System.out.println("result = " + result);
            context.write(new Text("dummyKey"), new Text(result));
        }
    }
}
