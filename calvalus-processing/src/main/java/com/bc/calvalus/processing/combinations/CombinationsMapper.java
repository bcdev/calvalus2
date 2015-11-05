package com.bc.calvalus.processing.combinations;

import com.bc.calvalus.processing.executable.ExecutableProcessorAdapter;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An mapper that a single combinations of the variables.
 */
public class CombinationsMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        InputSplit inputSplit = context.getInputSplit();
        if (!(inputSplit instanceof CombinationsInputSplit)) {
            throw new IllegalArgumentException("InputSplit is a " + inputSplit.getClass());
        }

        CombinationsInputSplit combinationsInputSplit = (CombinationsInputSplit) inputSplit;
        CombinationsConfig combinationsConfig = CombinationsConfig.get(conf);
        List<String> variableNames = combinationsConfig.getVariableNames();
        String[] values = combinationsInputSplit.getValues();

        Map<String, String> velocityProps = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            String name = variableNames.get(i);
            velocityProps.put(name, value);
        }
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm .beginTask("combinations", 100);
        ExecutableProcessorAdapter processorAdapter = new ExecutableProcessorAdapter(context);
        String[] outFiles = processorAdapter.processInput(SubProgressMonitor.create(pm, 95),
                                                          null,
                                                          new Path("dummy"),
                                                          new File("dummy"),
                                                          null,
                                                          velocityProps);

        if (outFiles != null && outFiles.length > 0) {
            processorAdapter.saveProcessedProductFiles(outFiles, SubProgressMonitor.create(pm, 5));
        }
        pm.done();
    }
}
