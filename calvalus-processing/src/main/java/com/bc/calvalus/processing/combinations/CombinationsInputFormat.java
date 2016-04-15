package com.bc.calvalus.processing.combinations;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.IllegalFormatConversionException;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

/**
 * An input format that creates input splits by iterating over all possible combinations of the variables.
 *
 * @author MarcoZ
 */
public class CombinationsInputFormat extends InputFormat<NullWritable, NullWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();


    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        CombinationsConfig combinationsConfig = CombinationsConfig.get(context.getConfiguration());
        List<InputSplit> inputSplits = createInputSplits(combinationsConfig);
        LOG.info(String.format("Configuration leads to %d splits", inputSplits.size()));

        if (combinationsConfig.getFormatName().isEmpty()) {
            return inputSplits;
        }
        List<InputSplit> remainingInputSplits = checkForExistingOutput(context, combinationsConfig.getFormatName(), inputSplits);
        LOG.info(String.format("Splits still to compute %d", remainingInputSplits.size()));
        return remainingInputSplits;
    }

    private List<InputSplit> checkForExistingOutput(JobContext context, String formatName, List<InputSplit> inputSplits) throws IOException {
        Configuration conf = context.getConfiguration();
        Path outputPath = SimpleOutputFormat.getOutputPath(context);
        FileSystem fs = outputPath.getFileSystem(conf);

        List<InputSplit> inputSplitsTodo = new ArrayList<>(inputSplits.size());
        for (InputSplit inputSplit : inputSplits) {
            if (!outputForSplitExists(inputSplit, formatName, outputPath, fs)) {
                inputSplitsTodo.add(inputSplit);
            }
        }
        return inputSplitsTodo;
    }

    static boolean outputForSplitExists(InputSplit inputSplit, String formatName, Path outputPath, FileSystem fs) throws IOException {
        CombinationsInputSplit split = (CombinationsInputSplit) inputSplit;
        String[] values = split.getValues();
        Path splitOutputPath = createSplitOutputPath(outputPath, formatName, values);
        return fs.exists(splitOutputPath);
    }

    static Path createSplitOutputPath(Path outputPath, String formatName, String... values) {
        Object[] objs = new Object[values.length];
        for (int i = 0; i < objs.length; i++) {
            String value = values[i];
            // if there are multiple values, pick the first
            String[] splits = value.split(",");
            value = splits[0].trim();
            try {
                objs[i] = Long.parseLong(value);
            } catch (NumberFormatException nfel) {
                try {
                    objs[i] = Double.parseDouble(value);
                } catch (NumberFormatException nfed) {
                    // strings maybe enclosed in Quotes ', "
                    if (value.startsWith("'") && value.endsWith("'") ||
                            value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    objs[i] = value;
                }
            }
        }
        try {
            String name = String.format(Locale.ENGLISH, formatName, objs);
            return new Path(outputPath, name);
        } catch (IllegalFormatConversionException e) {
            System.out.println(formatName);
            for (int i = 0; i < objs.length; i++) {
                Object obj = objs[i];
                System.out.printf("obj[%d] = %s (%s)%n", i, obj, obj.getClass());
            }
            throw e;
        }
    }

        static List<InputSplit> createInputSplits (CombinationsConfig combinationsConfig){
            return new Combiner(combinationsConfig.getVariables()).getSplits();
        }

        /**
         * Creates a {@link com.bc.calvalus.processing.hadoop.NoRecordReader} because records are not used with this input format.
         */
        @Override
        public RecordReader<NullWritable, NullWritable> createRecordReader (InputSplit split,
                TaskAttemptContext context)throws IOException,
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
                String loopLocation = variable.getLoopLocation();
                if (loopLocation != null && loopLocation.equalsIgnoreCase("task")) {
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
