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

    private final S2BaInputFormat delegate;

    public S2BaPostInputFormat() {
        delegate = new S2BaInputFormat();
    }

    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        List<InputSplit> preAndPostSplits = delegate.getSplits(jobContext);
        Configuration conf = jobContext.getConfiguration();
        String outputDir = jobContext.getConfiguration().get("calvalus.output.dir");
        String inputPathPatterns = outputDir + "/intermediate.*.nc";

        List<InputSplit> intermediateResultSplits = new ArrayList<>(1000);
        List<InputSplit> splits = new ArrayList<>(1000);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService inventoryService = new HdfsInventoryService(jobClientsMap, "eodata");
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        FileStatus[] fileStatuses = inventoryService.globFileStatuses(inputPatterns, conf);
        createSplits(fileStatuses, intermediateResultSplits);
        Logger.getLogger("com.bc.calvalus").info(String.format("Created %d intermediate split(s).", intermediateResultSplits.size()));

        for (InputSplit intermediateResultSplit : intermediateResultSplits) {
            CombineFileSplit intermediateSplit = (CombineFileSplit) intermediateResultSplit;
            String intermediateName = intermediateSplit.getPath(0).getName();
            String tile = intermediateName.substring(intermediateName.indexOf('-') + 1, intermediateName.indexOf('-') + 7);
            for (InputSplit preAndPostSplit : preAndPostSplits) {
                CombineFileSplit currentPreAndPostSplit = (CombineFileSplit) preAndPostSplit;

                Path referencePath = currentPreAndPostSplit.getPath(0);
                if (referencePath.getName().matches(".*" + tile + "\\.tif") && dateOfReferencePathMatchesPostDateOfIntermediate(referencePath.getName(), intermediateName)) {
                    List<Path> filePaths = new ArrayList<>();
                    List<Long> fileLengths = new ArrayList<>();
                    filePaths.add(intermediateSplit.getPath(0));
                    fileLengths.add(intermediateSplit.getLength(0));

                    Path[] paths = currentPreAndPostSplit.getPaths();
                    for (int i = 0; i < paths.length; i++) {
                        filePaths.add(currentPreAndPostSplit.getPath(i));
                        fileLengths.add(currentPreAndPostSplit.getLength(i));
                    }

                    splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                            fileLengths.stream().mapToLong(Long::longValue).toArray()));

                }
            }
        }

        return splits;
    }

    private boolean dateOfReferencePathMatchesPostDateOfIntermediate(String referenceName, String intermediateName) {
        String intermediateDate = intermediateName.substring(intermediateName.lastIndexOf('-') + 1, intermediateName.length() - 3);
        String referenceDate = referenceName.substring("S2A_USER_MTD_SAFL2A_PDMC_".length(), "S2A_USER_MTD_SAFL2A_PDMC_".length() + 15);
        return intermediateDate.equals(referenceDate);
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
        }
    }

    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return delegate.createRecordReader(inputSplit, taskAttemptContext);
    }
}
