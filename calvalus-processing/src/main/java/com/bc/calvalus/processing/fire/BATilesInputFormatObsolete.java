package com.bc.calvalus.processing.fire;

import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.mosaic.MosaicGrid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author thomas
 */
public class BATilesInputFormatObsolete extends ObsoleteFireInputFormat {

    @Override
    protected FileStatus[] getFileStatuses(HdfsInventoryService inventoryService,
                                           String inputPathPatterns,
                                           Configuration conf) throws IOException {
        FileStatus[] sameYearStatuses = super.getFileStatuses(inventoryService, inputPathPatterns, conf);

        String[] wingsPathPatterns = BATilesInputFormatObsolete.createWingsPathPatterns(inputPathPatterns);
        FileStatus[] beforeStatuses = wingsPathPatterns[0].length() > 0 ? super.getFileStatuses(inventoryService, wingsPathPatterns[0], conf) : new FileStatus[0];
        FileStatus[] afterStatuses = wingsPathPatterns[1].length() > 0 ? super.getFileStatuses(inventoryService, wingsPathPatterns[1], conf) : new FileStatus[0];

        FileStatus[] fileStatuses = new FileStatus[sameYearStatuses.length + beforeStatuses.length + afterStatuses.length];
        System.arraycopy(sameYearStatuses, 0, fileStatuses, 0, sameYearStatuses.length);
        System.arraycopy(beforeStatuses, 0, fileStatuses, sameYearStatuses.length, beforeStatuses.length);
        System.arraycopy(afterStatuses, 0, fileStatuses, sameYearStatuses.length + beforeStatuses.length, afterStatuses.length);
        return fileStatuses;
    }

    @Override
    protected void validatePattern(String inputPathPatterns) throws IOException {
        // example: hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/.*/2008/2008.*fire-nc/.*nc$
        String regex = "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/\\d\\d\\d\\d/.*/\\d\\d\\d\\d/\\d\\d\\d\\d.*fire-nc/.*nc\\$";
        if (!inputPathPatterns.matches(regex)) {
            throw new IOException("invalid input path; must match following pattern:\n" + regex);
        }
    }

    protected void createSplits(FileStatus[] fileStatuses,
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
    }

    static String[] createWingsPathPatterns(String inputPathPatterns) throws IOException {
        int year = Integer.parseInt(inputPathPatterns.substring(
                "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/".length(),
                "hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/".length() + 4));
        String beforeInputPathPatterns = "";
        if (year - 1 > 2001) {
            beforeInputPathPatterns = inputPathPatterns.replaceAll(Integer.toString(year), Integer.toString(year - 1))
                .replace(Integer.toString(year - 1) + ".*fire-nc",
                         Integer.toString(year - 1) + "-1[12]-.*fire-nc");
        }
        String afterInputPathPatterns = "";
        if (year + 1 < 2013) {
            afterInputPathPatterns = inputPathPatterns.replaceAll(Integer.toString(year), Integer.toString(year + 1))
                    .replace(Integer.toString(year + 1) + ".*fire-nc",
                            Integer.toString(year + 1) + "-0[12]-.*fire-nc");
        }
        return new String[] {beforeInputPathPatterns, afterInputPathPatterns};
    }

    static boolean fileMatchesTile(FileStatus file, int tileX, int tileY) {
        return file.getPath().getName().contains(String.format("-v%02dh%02d", tileX, tileY));
    }

}
