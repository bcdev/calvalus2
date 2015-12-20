package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.mosaic.MosaicGrid;
import com.bc.calvalus.processing.productinventory.ProductInventory;
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
 *
 * @author thomas
 */
public class BATilesInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
        validatePattern(inputPathPatterns);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(jobClientsMap, "eodata");

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] sameYearStatuses = getFileStatuses(hdfsInventoryService, inputPathPatterns, conf);

        String[] wingsPathPatterns = createWingsPathPatterns(inputPathPatterns);
        FileStatus[] beforeStatuses = getFileStatuses(hdfsInventoryService, wingsPathPatterns[0], conf);
        FileStatus[] afterStatuses = getFileStatuses(hdfsInventoryService, wingsPathPatterns[1], conf);

        FileStatus[] fileStatuses = new FileStatus[sameYearStatuses.length + beforeStatuses.length + afterStatuses.length];
        System.arraycopy(sameYearStatuses, 0, fileStatuses, 0, sameYearStatuses.length);
        System.arraycopy(beforeStatuses, 0, fileStatuses, sameYearStatuses.length, beforeStatuses.length);
        System.arraycopy(afterStatuses, 0, fileStatuses, sameYearStatuses.length + beforeStatuses.length, afterStatuses.length);

        createSplits(fileStatuses, splits, conf);
        return splits;
    }

    static String[] createWingsPathPatterns(String inputPathPatterns) throws IOException {
        int year = Integer.parseInt(inputPathPatterns.substring(
                "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/".length(),
                "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/".length() + 4));
        String beforeInputPathPatterns = "";
        if (year - 1 > 2001) {
            beforeInputPathPatterns = inputPathPatterns.replaceAll(Integer.toString(year), Integer.toString(year - 1))
                .replace(Integer.toString(year - 1) + ".*fire-nc",
                         Integer.toString(year - 1) + "-1[12]-\\d\\d-fire-nc")
                .replace(".*nc$", ".*nc\\$");
        }
        String afterInputPathPatterns = "";
        if (year + 1 < 2013) {
            afterInputPathPatterns = inputPathPatterns.replaceAll(Integer.toString(year), Integer.toString(year + 1))
                    .replace(Integer.toString(year + 1) + ".*fire-nc",
                            Integer.toString(year + 1) + "-0[12]-\\d\\d-fire-nc")
                    .replace(".*nc$", ".*nc\\$");
        }
        return new String[] {beforeInputPathPatterns, afterInputPathPatterns};
    }

    static void validatePattern(String inputPathPatterns) throws IOException {
        // example: hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/.*/2008/2008.*fire-nc/.*nc$
        String regex = "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/\\d\\d\\d\\d/\\.\\*/\\d\\d\\d\\d/\\d\\d\\d\\d.*fire-nc/.*nc\\$";
        if (!inputPathPatterns.matches(regex)) {
            throw new IOException("invalid input path; must match following pattern:\n" + regex);
        }
    }

    private void createSplits(FileStatus[] fileStatuses,
                              List<InputSplit> splits,
                              Configuration conf) throws IOException {
        // for all tiles, create an instance of CombineFileSplit

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

//
//
//        for (FileStatus file : fileStatuses) {
//            long fileLength = file.getLen();
//            FileSystem fs = file.getPath().getFileSystem(conf);
//            BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, fileLength);
//            if (blocks != null && blocks.length > 0) {
//                BlockLocation block = blocks[0];
//                // create a split for the input
//                if (productInventory == null) {
//                    // no inventory, process whole product
//                    splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts()));
//                } else {
//                    ProductInventoryEntry entry = productInventory.getEntry(file.getPath().getName());
//                    if (entry != null && entry.getProcessLength() > 0) {
//                        // when listed process the given subset
//                        int start = entry.getProcessStartLine();
//                        int length = entry.getProcessLength();
//                        splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts(), start, length));
//                    } else if (entry == null) {
//                        // when not listed process whole product
//                        splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts()));
//                    }
//                }
//            } else {
//                String msgFormat = "Failed to retrieve block location for file '%s'. Ignoring it.";
//                throw new IOException(String.format(msgFormat, file.getPath()));
//            }
//        }
    }

    private static boolean fileMatchesTile(FileStatus file, int tileX, int tileY) {
        return file.getPath().getName().contains(String.format("-v%02dh%02d", tileX, tileY));
    }

    private static FileStatus[] getFileStatuses(HdfsInventoryService inventoryService,
                                                String inputPathPatterns,
                                                Configuration conf) throws IOException {
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return inventoryService.globFileStatuses(inputPatterns, conf);
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }

    private static class CombineFileSplitDef {

        public CombineFileSplitDef() {
            this.files = new ArrayList<>();
            this.lengths = new ArrayList<>();
        }

        ArrayList<Path> files;
        ArrayList<Long> lengths;
    }
}
