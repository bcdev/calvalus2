package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class S2BaPostInputFormat extends InputFormat {

    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        Configuration conf = jobContext.getConfiguration();
        String outputDir = jobContext.getConfiguration().get("calvalus.output.dir");
        String inputPathPatterns = outputDir + "/intermediate.*.nc";

        List<InputSplit> splits = new ArrayList<>(1000);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService inventoryService = new HdfsInventoryService(jobClientsMap, "eodata");
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        FileStatus[] fileStatuses = inventoryService.globFileStatuses(inputPatterns, conf);
        createSplits(fileStatuses, splits);
        Logger.getLogger("com.bc.calvalus").info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void createSplits(FileStatus[] fileStatuses, List<InputSplit> splits) throws IOException {
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());
            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
            Logger.getLogger("com.bc.calvalus").info(String.format("Added file %s", fileStatus.getPath().getName()));
        }
    }

    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReader() {
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            }

            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }

            public Object getCurrentKey() throws IOException, InterruptedException {
                return null;
            }

            public Object getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

            public void close() throws IOException {
            }
        };
    }
}
