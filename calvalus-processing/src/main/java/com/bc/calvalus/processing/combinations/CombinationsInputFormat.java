package com.bc.calvalus.processing.combinations;

import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An input format that creates input splits by iterating over all possible combinations of the variables.
 *
 * @author MarcoZ
 */
public class CombinationsInputFormat extends InputFormat<NullWritable, NullWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        CombinationsConfig combinationsConfig = CombinationsConfig.get(conf);
        return createInputSplits(combinationsConfig);
    }

    static List<InputSplit> createInputSplits(CombinationsConfig combinationsConfig) {
        return new Combiner(combinationsConfig.getVariables()).getSplits();
    }

    /**
     * Creates a {@link com.bc.calvalus.processing.hadoop.NoRecordReader} because records are not used with this input format.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
                                                                       TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new NoRecordReader();
    }

    private static class Combiner {

        private final CombinationsConfig.Variable[] variables;
        private final List<InputSplit> inputSplits;

        private Combiner(CombinationsConfig.Variable[] variables) {
            this.variables = variables;
            this.inputSplits = new ArrayList<>();
        }

        private List<InputSplit> getSplits() {
            String[] combination = new String[variables.length];
            recurse(combination, 0);
            return inputSplits;
        }

        private void recurse(String[] combination, int index) {
            CombinationsConfig.Variable variable = variables[index];
            if (variable.getLoopLocation().equals("task")) {
                String value = StringUtils.arrayToString(variable.getValues());
                String[] combinationCopy = Arrays.copyOf(combination, combination.length);
                combinationCopy[index] = value;
                if (index < variables.length - 1) {
                    recurse(combinationCopy, index + 1);
                } else {
                    inputSplits.add(new CombinationsInputSplit(combinationCopy));
                }
            } else {
                String[] values = variable.getValues();
                for (String value : values) {
                    String[] combinationCopy = Arrays.copyOf(combination, combination.length);
                    combinationCopy[index] = value;
                    if (index < variables.length - 1) {
                        recurse(combinationCopy, index + 1);
                    } else {
                        inputSplits.add(new CombinationsInputSplit(combinationCopy));
                    }
                }
            }
        }
    }
}
