package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class S2BaInputFormat extends InputFormat {

    private static final int MAX_PRE_IMAGES_COUNT_SINGLE_ORBIT = 4;
    private static final int MAX_PRE_IMAGES_COUNT_MULTI_ORBIT = 8;

    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        Configuration conf = jobContext.getConfiguration();
        String inputPathPatterns = conf.get("calvalus.input.pathPatterns");

        Set<InputSplit> splits = new HashSet<>(1000);

        HdfsInventoryService inventoryService = new HdfsInventoryService(conf, "eodata");
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        FileStatus[] fileStatuses = inventoryService.globFileStatuses(inputPatterns, conf);

        // for each split, a single mapper is run
        // --> fileStatuses must contain filestatus for each input product at this stage
        createSplits(fileStatuses, splits, inventoryService, conf);
        // here, each split must contain two files: pre and post period.
        Logger.getLogger("com.bc.calvalus").info(String.format("Created %d split(s).", splits.size()));
        return Arrays.asList(splits.toArray(new InputSplit[0]));
    }

    private void createSplits(FileStatus[] fileStatuses,
                              Set<InputSplit> splits, HdfsInventoryService inventoryService, Configuration conf) throws IOException {
        /*
        for each file status r:
            take r and (up to) latest 4 or 8 matching files d, c, b, a (getPeriodStatuses)
                create r, d
                create r, c
                create r, b
                create r, a
         */
        for (FileStatus referenceFileStatus : fileStatuses) {
            FileStatus[] periodStatuses = getPeriodStatuses(referenceFileStatus, inventoryService, conf);
            for (FileStatus preStatus : periodStatuses) {
                splits.add(createSplit(referenceFileStatus, preStatus));
                Logger.getLogger("com.bc.calvalus").info(String.format("Created split with postStatus %s and preStatus %s.", getDate(referenceFileStatus), getDate(preStatus)));
            }
        }
    }

    private static CombineFileSplit createSplit(FileStatus postFileStatus, FileStatus preStatus) {
        List<Path> filePaths = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();
        filePaths.add(postFileStatus.getPath());
        fileLengths.add(postFileStatus.getLen());
        filePaths.add(preStatus.getPath());
        fileLengths.add(preStatus.getLen());
        return new ComparableCombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                fileLengths.stream().mapToLong(Long::longValue).toArray());
    }

    private FileStatus[] getPeriodStatuses(FileStatus referenceFileStatus, HdfsInventoryService inventoryService, Configuration conf) throws IOException {
        String referencePath = referenceFileStatus.getPath().toString(); // hdfs://calvalus/calvalus/projects/fire/s2-pre/2016/01/16/S2A_USER_MTD_SAFL2A_PDMC_20160116T175154_R108_V20160116T105012_20160116T105012_T30PVR.tif
        String periodInputPathPattern = getPeriodInputPathPattern(referencePath);

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(periodInputPathPattern);
        FileStatus[] periodStatuses = inventoryService.globFileStatuses(inputPatterns, conf);
        sort(periodStatuses);

        List<FileStatus> filteredList = new ArrayList<>();
        for (FileStatus periodStatus : periodStatuses) {
            if (getDate(periodStatus).getTime() < getDate(referenceFileStatus).getTime()) {
                filteredList.add(periodStatus);
            }
        }

        int maxPreImagesCount = getMaxPreImagesCount(filteredList);
        int resultCount = Math.min(maxPreImagesCount, filteredList.size());
        FileStatus[] result = new FileStatus[resultCount];
        System.arraycopy(filteredList.toArray(new FileStatus[0]), 0, result, 0, resultCount);
        return result;
    }

    static int getMaxPreImagesCount(List<FileStatus> filteredList) {
        Logger.getLogger("com.bc.calvalus").info("Computing max pre images count...");
        String orbit = null;
        for (FileStatus fileStatus : filteredList) {
            if (orbit == null) {
                orbit = fileStatus.getPath().getName().substring(42, 47);
            } else {
                if (!orbit.equals(fileStatus.getPath().getName().substring(42, 47))) {
                    Logger.getLogger("com.bc.calvalus").info("...other file found with different orbit, so it is 8.");
                    return MAX_PRE_IMAGES_COUNT_MULTI_ORBIT;
                }
            }
        }
        Logger.getLogger("com.bc.calvalus").info("...no file found with different orbit, so it is 4.");
        return MAX_PRE_IMAGES_COUNT_SINGLE_ORBIT;
    }

    private static void sort(FileStatus[] periodStatuses) {
        Arrays.sort(periodStatuses, (fs1, fs2) -> getDate(fs1).getTime() > getDate(fs2).getTime() ? -1 : 1);
    }

    private static Date getDate(FileStatus fs) {
        String fsName = fs.getPath().getName(); // S2A_USER_MTD_SAFL2A_PDMC_20160116T175154_R108_V20160116T105012_20160116T105012_T30PVR.tif
        fsName = fsName.substring("S2A_USER_MTD_SAFL2A_PDMC_20160116T175154_R108_V".length(), "S2A_USER_MTD_SAFL2A_PDMC_20160116T175154_R108_V".length() + 15); // 20160116T105012
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

    @SuppressWarnings("WeakerAccess")
    public static class ComparableCombineFileSplit extends CombineFileSplit {

        @SuppressWarnings("unused")
        public ComparableCombineFileSplit() {
        }

        public ComparableCombineFileSplit(Path[] files, long[] lengths) {
            super(files, lengths);
        }

        @Override
        public boolean equals(Object obj) {
            return this.toString().equals(obj.toString());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(toString().toCharArray());
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
