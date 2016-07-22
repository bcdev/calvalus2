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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

public class S2BaInputFormat extends InputFormat {

    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        Configuration conf = jobContext.getConfiguration();
        String inputPathPatterns = conf.get("calvalus.input.pathPatterns");

        List<InputSplit> splits = new ArrayList<>(1000);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService inventoryService = new HdfsInventoryService(jobClientsMap, "eodata");
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        FileStatus[] fileStatuses = inventoryService.globFileStatuses(inputPatterns, conf);

        // for each split, a single mapper is run
        // --> fileStatuses must contain filestatus for each input product at this stage
        createSplits(fileStatuses, splits, inventoryService, conf);
        // here, each split must contain two files: pre and post period.
        Logger.getLogger("com.bc.calvalus").info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void createSplits(FileStatus[] fileStatuses,
                              List<InputSplit> splits, HdfsInventoryService inventoryService, Configuration conf) throws IOException {
        for (FileStatus referenceFileStatus : fileStatuses) {
            Path path = referenceFileStatus.getPath();
            FileStatus[] periodStatuses = getPeriodStatuses(referenceFileStatus, path, inventoryService, conf);
            for (FileStatus preStatus : periodStatuses) {
                List<Path> filePaths = new ArrayList<>();
                List<Long> fileLengths = new ArrayList<>();
                filePaths.add(path);
                fileLengths.add(referenceFileStatus.getLen());
                filePaths.add(preStatus.getPath());
                fileLengths.add(preStatus.getLen());
                splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                        fileLengths.stream().mapToLong(Long::longValue).toArray()));
                Logger.getLogger("com.bc.calvalus").info(String.format("Added reference file %s and preStatus %s.", referenceFileStatus.getPath().getName(), preStatus.getPath().getName()));
            }
        }
    }

    private FileStatus[] getPeriodStatuses(FileStatus referenceFileStatus, Path path, HdfsInventoryService inventoryService, Configuration conf) throws IOException {
        String s2PrePath = path.toString(); // hdfs://calvalus/calvalus/projects/fire/s2-pre/2016/01/16/S2A_USER_MTD_SAFL2A_PDMC_20160116T175154_R108_V20160116T105012_20160116T105012_T30PVR.tif
        String periodInputPathPattern = getPeriodInputPathPattern(s2PrePath);

        Logger.getLogger("com.bc.calvalus").info("periodInputPathPattern=" + periodInputPathPattern);
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(periodInputPathPattern);
        FileStatus[] periodStatuses = inventoryService.globFileStatuses(inputPatterns, conf);
        for (FileStatus periodStatus : periodStatuses) {
            Logger.getLogger("com.bc.calvalus").info("periodStatus=" + periodStatus);
        }
        sort(periodStatuses);

        List<FileStatus> filteredList = new ArrayList<>();
        for (FileStatus periodStatus : periodStatuses) {
            if (getDate(periodStatus).before(getDate(referenceFileStatus))) {
                filteredList.add(periodStatus);
            }
        }
        int resultCount = Math.min(4, filteredList.size());
        FileStatus[] result = new FileStatus[resultCount];
        System.arraycopy(filteredList.toArray(new FileStatus[0]), 0, result, 0, resultCount);
        return result;
    }

    private static void sort(FileStatus[] periodStatuses) {
        Arrays.sort(periodStatuses, (fs1, fs2) -> getDate(fs1).after(getDate(fs2)) ? 0 : 1);
    }

    private static Date getDate(FileStatus fs) {
        String fsName = fs.getPath().getName(); // S2A_USER_MTD_SAFL2A_PDMC_20160116T175154_R108_V20160116T105012_20160116T105012_T30PVR.tif
        fsName = fsName.substring("S2A_USER_MTD_SAFL2A_PDMC_".length(), "S2A_USER_MTD_SAFL2A_PDMC_".length() + 15); // 20160116T175154
        try {
            return new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(fsName);
        } catch (ParseException e) {
            throw new IllegalStateException("Programming error in input format, see wrapped exception", e);
        }
    }

    static String getPeriodInputPathPattern(String s2PrePath) {
        int yearIndex = s2PrePath.indexOf("s2-pre/") + "s2-pre/".length();
        int tileIndex = s2PrePath.indexOf(".tif") - 6;
        String tile = s2PrePath.substring(tileIndex, tileIndex + 6);
        String basePath = s2PrePath.substring(0, yearIndex);
        return String.format("%s.*/.*/.*/.*%s.tif$", basePath, tile);
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
