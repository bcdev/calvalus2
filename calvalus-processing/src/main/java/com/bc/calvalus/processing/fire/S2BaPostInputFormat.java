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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

public class S2BaPostInputFormat extends InputFormat {

    private final S2BaInputFormat delegate;

    public S2BaPostInputFormat() {
        delegate = new S2BaInputFormat();
    }

    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        Configuration conf = jobContext.getConfiguration();
        String outputDir = jobContext.getConfiguration().get("calvalus.output.dir");
        String inputPathPatterns = outputDir + "/intermediate.*.nc";

        List<CombineFileSplit> intermediateResultSplits = new ArrayList<>(1000);
        List<InputSplit> splits = new ArrayList<>(1000);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService inventoryService = new HdfsInventoryService(jobClientsMap, "eodata");
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        FileStatus[] fileStatuses = inventoryService.globFileStatuses(inputPatterns, conf);
        createSplits(fileStatuses, intermediateResultSplits);
        Logger.getLogger("com.bc.calvalus").info(String.format("Created %d intermediate split(s).", intermediateResultSplits.size()));
        intermediateResultSplits.sort(new InputSplitComparator());

        for (InputSplit referenceResultSplit : intermediateResultSplits) {
            CombineFileSplit split = (CombineFileSplit) referenceResultSplit;
            String intermediateName = split.getPath(0).getName();
            String tile = intermediateName.substring(intermediateName.indexOf('-') + 1, intermediateName.indexOf('-') + 7);
            String currentPostDateString = intermediateName.substring(intermediateName.lastIndexOf('-') + 1, intermediateName.length() - 3);
            LocalDate referenceDate = LocalDate.parse(currentPostDateString, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss"));

            int count = 0;
            for (InputSplit comparedResultSplit : intermediateResultSplits) {
                CombineFileSplit compareSplit = (CombineFileSplit) comparedResultSplit;
                if (compareSplit == referenceResultSplit) {
                    continue;
                }
                String comparedResultName = compareSplit.getPath(0).getName();
                boolean tileMatches = comparedResultName.matches(".*" + tile + "\\.tif");
                boolean notMoreThanFour = count <= S2BaInputFormat.MAX_PRE_IMAGES_COUNT;
                String comparedResultNamePostDateString = comparedResultName.substring(comparedResultName.lastIndexOf('-') + 1, comparedResultName.length() - 3);
                LocalDate compareDate = LocalDate.parse(comparedResultNamePostDateString, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss"));

                if (tileMatches && compareDate.isBefore(referenceDate) && notMoreThanFour) {
                    count++;
                    List<Path> filePaths = new ArrayList<>();
                    List<Long> fileLengths = new ArrayList<>();
                    filePaths.add(compareSplit.getPath(0));
                    fileLengths.add(compareSplit.getLength(0));
                    splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                            fileLengths.stream().mapToLong(Long::longValue).toArray()));
                }
            }
        }

        return splits;
    }

    private void createSplits(FileStatus[] fileStatuses, List<CombineFileSplit> splits) throws IOException {
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

    private static class InputSplitComparator implements Comparator<CombineFileSplit> {
        @Override
        public int compare(CombineFileSplit o1, CombineFileSplit o2) {
            String o1Name = o1.getPath(0).getName();
            String o2Name = o2.getPath(0).getName();
            String o1DateString = o1Name.substring(o1Name.lastIndexOf('-') + 1, o1Name.length() - 3);
            String o2DateString = o2Name.substring(o2Name.lastIndexOf('-') + 1, o2Name.length() - 3);
            LocalDate o1Date = LocalDate.parse(o1DateString, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss"));
            LocalDate o2Date = LocalDate.parse(o2DateString, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss"));
            return -1 * o1Date.compareTo(o2Date);
        }
    }
}
