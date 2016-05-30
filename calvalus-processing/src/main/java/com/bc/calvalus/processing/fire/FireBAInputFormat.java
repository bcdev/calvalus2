package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.mosaic.MosaicGrid;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author thomas
 */
public class FireBAInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
        validatePattern(inputPathPatterns);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(jobClientsMap, "eodata");

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getAllFileStatuses(hdfsInventoryService, inputPathPatterns, conf);

        createSplits(fileStatuses, splits, conf);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private FileStatus[] getAllFileStatuses(HdfsInventoryService inventoryService,
                                            String inputPathPattern,
                                            Configuration conf) throws IOException {
        FileStatus[] sameYearStatuses = getFileStatuses(inventoryService, inputPathPattern, conf);

        String[] wingsPathPatterns = createWingsPathPatterns(inputPathPattern);
        FileStatus[] beforeStatuses = wingsPathPatterns[0].length() > 0 ? getFileStatuses(inventoryService, wingsPathPatterns[0], conf) : new FileStatus[0];
        FileStatus[] afterStatuses = wingsPathPatterns[1].length() > 0 ? getFileStatuses(inventoryService, wingsPathPatterns[1], conf) : new FileStatus[0];
        String auxdataPathPattern = createAuxdataPathPattern(inputPathPattern);
        FileStatus[] auxdataStatuses = getFileStatuses(inventoryService, auxdataPathPattern, conf);

        FileStatus[] allFileStatuses = new FileStatus[sameYearStatuses.length + beforeStatuses.length + afterStatuses.length + auxdataStatuses.length];
        System.arraycopy(sameYearStatuses, 0, allFileStatuses, 0, sameYearStatuses.length);
        System.arraycopy(beforeStatuses, 0, allFileStatuses, sameYearStatuses.length, beforeStatuses.length);
        System.arraycopy(afterStatuses, 0, allFileStatuses, sameYearStatuses.length + beforeStatuses.length, afterStatuses.length);
        System.arraycopy(auxdataStatuses, 0, allFileStatuses, sameYearStatuses.length + beforeStatuses.length + afterStatuses.length, auxdataStatuses.length);
        return allFileStatuses;
    }


    private void createSplits(FileStatus[] fileStatuses,
                              List<InputSplit> splits,
                              Configuration conf) throws IOException {

        MosaicGrid mosaicGrid = MosaicGrid.create(conf);
        int numTilesX = mosaicGrid.getNumTileX();
        int numTilesY = mosaicGrid.getNumTileY();

        Map<String, CombineFileSplitDef> splitMap = new HashMap<>();
        for (FileStatus file : fileStatuses) {
            for (int tileX = 0; tileX < numTilesX; tileX++) {
                for (int tileY = 0; tileY < numTilesY; tileY++) {
                    if (fileMatchesTile(file, tileX, tileY)) {
                        CombineFileSplitDef inputSplitDef;
                        String key = String.format("v%02dh%02d", tileX, tileY);
                        if (splitMap.containsKey(key)) {
                            inputSplitDef = splitMap.get(key);
                        } else {
                            inputSplitDef = new CombineFileSplitDef();
                            splitMap.put(key, inputSplitDef);
                        }
                        inputSplitDef.files.add(file.getPath());
                        inputSplitDef.lengths.add(file.getLen());
                    }
                }
            }
        }
        for (CombineFileSplitDef combineFileSplitDef : splitMap.values()) {
            Path[] files = combineFileSplitDef.files.toArray(new Path[combineFileSplitDef.files.size()]);
            long[] lengths = combineFileSplitDef.lengths.stream().mapToLong(Long::longValue).toArray();
            splits.add(new CombineFileSplit(files, lengths));
        }
    }

    private FileStatus[] getFileStatuses(HdfsInventoryService inventoryService,
                                         String inputPathPatterns,
                                         Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return inventoryService.globFileStatuses(inputPatterns, conf);
    }

    private static void validatePattern(String inputPathPatterns) throws IOException {
        // example: hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/.*/2008/2008.*fire-nc/.*nc$
        String regex = "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/\\d\\d\\d\\d/.*/\\d\\d\\d\\d/\\d\\d\\d\\d.*fire-nc/.*nc\\$";
        if (!inputPathPatterns.matches(regex)) {
            throw new IOException("invalid input path; must match following pattern:\n" + regex);
        }
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }

    private static class CombineFileSplitDef {

        CombineFileSplitDef() {
            this.files = new ArrayList<>();
            this.lengths = new ArrayList<>();
        }

        ArrayList<Path> files;
        ArrayList<Long> lengths;
    }

    private static String[] createWingsPathPatterns(String inputPathPattern) throws IOException {
        int year = Integer.parseInt(inputPathPattern.substring(
                "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/".length(),
                "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/".length() + 4));
        String beforeInputPathPatterns = "";
        if (year - 1 > 2001) {
            beforeInputPathPatterns = inputPathPattern.replaceAll(Integer.toString(year), Integer.toString(year - 1))
                    .replace(Integer.toString(year - 1) + ".*fire-nc",
                            Integer.toString(year - 1) + "-1[12]-.*fire-nc");
        }
        String afterInputPathPatterns = "";
        if (year + 1 < 2013) {
            afterInputPathPatterns = inputPathPattern.replaceAll(Integer.toString(year), Integer.toString(year + 1))
                    .replace(Integer.toString(year + 1) + ".*fire-nc",
                            Integer.toString(year + 1) + "-0[12]-.*fire-nc");
        }
        return new String[]{beforeInputPathPatterns, afterInputPathPatterns};
    }

    private static String createAuxdataPathPattern(String inputPathPattern) throws IOException {
        int year = Integer.parseInt(inputPathPattern.substring(
                "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/".length(),
                "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/".length() + 4));
        return String.format("hdfs://calvalus/calvalus/projects/fire/meris-ba/%d/auxdata-%d-.*gz", year, year);
    }

    private static boolean fileMatchesTile(FileStatus file, int tileX, int tileY) {
        return file.getPath().getName().contains(String.format("-v%02dh%02d", tileX, tileY));
    }

}
