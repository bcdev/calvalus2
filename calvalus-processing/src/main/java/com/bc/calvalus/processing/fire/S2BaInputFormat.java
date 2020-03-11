package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
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
        String tile = conf.get("calvalus.tile");

        Set<InputSplit> splits = new HashSet<>(1000);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        FileStatus[] fileStatuses = hdfsInventoryService.globFileStatuses(inputPatterns, conf);

        // for each split, a single mapper is run
        // --> fileStatuses must contain filestatus for each input product at this stage
        createSplits(fileStatuses, tile, splits, hdfsInventoryService, conf);
        // here, each split must contain two files: pre and post period.
        Logger.getLogger("com.bc.calvalus").info(String.format("Created %d split(s).", splits.size()));
        return Arrays.asList(splits.toArray(new InputSplit[0]));
    }

    private void createSplits(FileStatus[] fileStatuses, String tile,
                              Set<InputSplit> splits, HdfsFileSystemService hdfsInventoryService, Configuration conf) throws IOException {
        /*
        for each file status r:
            take r and (up to) latest 4 or 8 matching files d, c, b, a (getPeriodStatuses)
                create r, d
                create r, c
                create r, b
                create r, a
         */
        for (FileStatus referenceFileStatus : fileStatuses) {
            // if there already exists a file, like BA-T37NCF-20160514T075819.nc: continue
            Date referenceDate = getDate(referenceFileStatus);
            String postDate = new SimpleDateFormat("yyyyMMdd'T'HHmmss").format(referenceDate);
            String path = "hdfs://calvalus/calvalus/projects/fire/s2-ba/" + tile + "/BA-" + tile + "-" + postDate + ".nc";
            Logger.getLogger("com.bc.calvalus").info("Checking if BA output file '" + path + "' already exists...");
            if (hdfsInventoryService.pathExists(path, "cvop")) {
                Logger.getLogger("com.bc.calvalus").info("already exists, moving to next reference date.");
                continue;
            }
            Logger.getLogger("com.bc.calvalus").info("does not already exist, create splits accordingly.");
            FileStatus[] periodStatuses = getPeriodStatuses(referenceFileStatus, hdfsInventoryService, conf);
            for (FileStatus preStatus : periodStatuses) {
                splits.add(createSplit(referenceFileStatus, preStatus));
                Logger.getLogger("com.bc.calvalus").info(String.format("Created split with postStatus %s and preStatus %s.", referenceDate, getDate(preStatus)));
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

    private FileStatus[] getPeriodStatuses(FileStatus referenceFileStatus, HdfsFileSystemService hdfsInventoryService, Configuration conf) throws IOException {
        String referencePath = referenceFileStatus.getPath().toString();
        String periodInputPathPattern = getPeriodInputPathPattern(referencePath);

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(periodInputPathPattern);
        FileStatus[] periodStatuses = hdfsInventoryService.globFileStatuses(inputPatterns, conf);
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
                orbit = fileStatus.getPath().getName().substring(34, 39);
            } else {
                if (!orbit.equals(fileStatus.getPath().getName().substring(34, 39))) {
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
        String fsName = fs.getPath().getName();
        String datePart = fsName.split("_")[2];

        try {
            return new SimpleDateFormat("yyyyMMdd'T'HHmmss").parse(datePart);
        } catch (ParseException e) {
            throw new IllegalStateException("Programming error in input format, see wrapped exception", e);
        }
    }

    static String getPeriodInputPathPattern(String s2PrePath) {
        int yearIndex = s2PrePath.indexOf("s2-pre/") + "s2-pre/".length();
        String tile = s2PrePath.split("_")[5];
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

    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new RecordReader() {
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
            }

            public boolean nextKeyValue() {
                return false;
            }

            public Object getCurrentKey() {
                return null;
            }

            public Object getCurrentValue() {
                return null;
            }

            public float getProgress() {
                return 0;
            }

            public void close() {
            }
        };
    }
}
