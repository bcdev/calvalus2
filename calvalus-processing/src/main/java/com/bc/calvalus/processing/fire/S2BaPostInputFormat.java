package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class S2BaPostInputFormat extends InputFormat {

    private final S2BaInputFormat delegate;

    public S2BaPostInputFormat() {
        delegate = new S2BaInputFormat();
    }

    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        Configuration conf = jobContext.getConfiguration();
        String inputDir = jobContext.getConfiguration().get("calvalus.input.dir");
        String outputDir = jobContext.getConfiguration().get("calvalus.output.dir");
        String tile = jobContext.getConfiguration().get("calvalus.tile");
        if (!tile.startsWith("T")) {
            tile = "T" + tile;
        }
        if (tile.startsWith("TT")) {
            tile = tile.replace("TT", "T");
        }
        String sensor = jobContext.getConfiguration().get("calvalus.sensor");
        String inputPathPattern = String.format("%s/%s/intermediate-%s-%s.*.nc", inputDir, tile, sensor, tile);

        List<CombineFileSplit> intermediateResultSplits = new ArrayList<>(1000);
        List<InputSplit> splits = new ArrayList<>(1000);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPattern);
        System.out.println("inputPatterns = " + inputPatterns);
        FileStatus[] fileStatuses = hdfsInventoryService.globFiles(jobContext.getUser(), inputPatterns);
        createSplits(fileStatuses, intermediateResultSplits);
        Logger.getLogger("com.bc.calvalus").info(String.format("Created %d intermediate split(s).", intermediateResultSplits.size()));

        List<String> alreadyHandledDates = new ArrayList<>();

        for (InputSplit referenceResultSplit : intermediateResultSplits) {
            CombineFileSplit split = (CombineFileSplit) referenceResultSplit;
            String referenceName = split.getPath(0).getName();
            String currentPostDateString = referenceName.substring(referenceName.lastIndexOf('-') + 1, referenceName.length() - 3);
            if (alreadyHandledDates.contains(currentPostDateString)) {
                continue;
            }
            alreadyHandledDates.add(currentPostDateString);

            String currentPathPattern = String.format("%s/%s/intermediate-%s-%s.*%s.nc", outputDir, tile, sensor, tile, currentPostDateString);
            List<String> currentPattern = inputPathResolver.resolve(currentPathPattern);
            FileStatus[] matchingStatuses = hdfsInventoryService.globFiles(jobContext.getUser(), currentPattern);
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            for (FileStatus matchingStatus : matchingStatuses) {
                filePaths.add(matchingStatus.getPath());
                fileLengths.add(matchingStatus.getLen());
            }
            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
        }

        for (InputSplit split : splits) {
            System.out.println(Arrays.toString(split.getLocations()));
            System.out.println(split.getLength());
            System.out.println("##########");
        }

        return splits;
    }

    private void createSplits(FileStatus[] fileStatuses, List<CombineFileSplit> splits) {
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());
            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
        }
    }

    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return delegate.createRecordReader(inputSplit, taskAttemptContext);
    }
}
