package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
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

/**
 * @author thomas
 */
public class FireInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
        validatePattern(inputPathPatterns);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(jobClientsMap, "eodata");

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getFileStatuses(hdfsInventoryService, inputPathPatterns, conf);

        createSplits(fileStatuses, splits, conf);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    protected void validatePattern(String inputPathPatterns) throws IOException {
        // do nothing
    }

    protected void createSplits(FileStatus[] fileStatuses,
                              List<InputSplit> splits,
                              Configuration conf) throws IOException {

        List<Path> filePaths = new ArrayList<>(fileStatuses.length);
        List<Long> fileLengths = new ArrayList<>(fileStatuses.length);
        for (FileStatus fileStatus : fileStatuses) {
            filePaths.add(fileStatus.getPath());
            fileLengths.add(fileStatus.getLen());
        }
        splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                                        fileLengths.stream().mapToLong(Long::longValue).toArray()));
    }

    static boolean fileMatchesTile(FileStatus file, int tileX, int tileY) {
        return file.getPath().getName().contains(String.format("-v%02dh%02d", tileX, tileY));
    }

    protected FileStatus[] getFileStatuses(HdfsInventoryService inventoryService,
                                            String inputPathPatterns,
                                            Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return inventoryService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }

    protected static class CombineFileSplitDef {

        public CombineFileSplitDef() {
            this.files = new ArrayList<>();
            this.lengths = new ArrayList<>();
        }

        ArrayList<Path> files;
        ArrayList<Long> lengths;
    }

}
