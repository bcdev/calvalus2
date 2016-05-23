package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
 * @author marcop
 */
public class GridInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(jobClientsMap, "eodata");

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getFileStatuses(hdfsInventoryService, inputPathPatterns, conf);

        createSplits(fileStatuses, splits, conf);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void createSplits(FileStatus[] fileStatuses,
                              List<InputSplit> splits, Configuration conf) throws IOException {

        List<String> usedTiles = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            Path path = fileStatus.getPath();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());
            FileStatus lcPath = getLcFileStatus(path, path.getFileSystem(conf));
            filePaths.add(lcPath.getPath());
            fileLengths.add(lcPath.getLen());
            FileStatus[] srPath = getSrFileStatuses(path, path.getFileSystem(conf));
            for (FileStatus status : srPath) {
                filePaths.add(status.getPath());
                fileLengths.add(status.getLen());
            }

            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
            usedTiles.add(getTile(path.toString()));
        }
        for (String tile : getMissingTiles(usedTiles)) {
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            // dummy for BA input
            filePaths.add(new Path("dummy"));
            fileLengths.add(0L);

            // dummy for LC input
            filePaths.add(new Path("dummy"));
            fileLengths.add(0L);

            // srStatuses
            FileStatus[] srPaths = getSrFileStatuses(conf.get("calvalus.year"), conf.get("calvalus.month"), tile, FileSystem.get(conf));
            for (FileStatus status : srPaths) {
                filePaths.add(status.getPath());
                fileLengths.add(status.getLen());
            }

            boolean hasSrData = srPaths.length > 0;
            if (hasSrData) {
                splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                        fileLengths.stream().mapToLong(Long::longValue).toArray()));
            }
        }
    }

    private FileStatus getLcFileStatus(Path path, FileSystem fileSystem) throws IOException {
        String baInputPath = path.toString(); // hdfs://calvalus/calvalus/projects/fire/meris-ba/$year/BA_PIX_MER_$tile_$year$month_v4.0.tif
        String lcInputPath = getLcInputPath(baInputPath);
        return fileSystem.getFileStatus(new Path(lcInputPath));
    }

    private FileStatus[] getSrFileStatuses(Path path, FileSystem fileSystem) throws IOException {
        String baInputPath = path.toString(); // hdfs://calvalus/calvalus/projects/fire/meris-ba/$year/BA_PIX_MER_$tile_$year$month_v4.0.tif
        String srInputPathPattern = getSrInputPathPattern(baInputPath);
        return fileSystem.globStatus(new Path(srInputPathPattern));
    }

    private FileStatus[] getSrFileStatuses(String year, String month, String tile, FileSystem fileSystem) throws IOException {
        String baInputPath = String.format("hdfs://calvalus/calvalus/projects/fire/meris-ba/%s/BA_PIX_MER_%s_%s%s_v4.0.tif", year, tile, year, month);
        String srInputPathPattern = getSrInputPathPattern(baInputPath);
        return fileSystem.globStatus(new Path(srInputPathPattern));
    }

    static String getLcInputPath(String baInputPath) {
        int yearIndex = baInputPath.indexOf("meris-ba/") + "meris-ba/".length();
        int year = Integer.parseInt(baInputPath.substring(yearIndex, yearIndex + 4));
        String lcYear = lcYear(year);
        int tileIndex = yearIndex + 4 + "/BA_PIX_MER_".length();
        String tile = baInputPath.substring(tileIndex, tileIndex + 6);
        return baInputPath.substring(0, baInputPath.indexOf("meris-ba")) + "aux/lc/" + String.format("lc-%s-%s.nc", lcYear, tile);
    }

    static String getTile(String baPath) {
        int startIndex = baPath.indexOf("BA_PIX_MER_") + "BA_PIX_MER_".length();
        return baPath.substring(startIndex, startIndex + 6);
    }

    static List<String> getMissingTiles(List<String> usedTiles) {
        List<String> missingTiles = new ArrayList<>();
        for (int v = 0; v <= 17; v++) {
            for (int h = 0; h <= 35; h++) {
                String tile = String.format("v%02dh%02d", v, h);
                if (!usedTiles.contains(tile)) {
                    missingTiles.add(tile);
                }
            }
        }
        return missingTiles;
    }

    static String getSrInputPathPattern(String baInputPath) {
        int yearIndex = baInputPath.indexOf("meris-ba/") + "meris-ba/".length();
        int monthIndex = baInputPath.indexOf("BA_PIX_MER") + 22;
        int year = Integer.parseInt(baInputPath.substring(yearIndex, yearIndex + 4));
        int month = Integer.parseInt(baInputPath.substring(monthIndex, monthIndex + 2));
        int tileIndex = yearIndex + 4 + "/BA_PIX_MER_".length();
        String tile = baInputPath.substring(tileIndex, tileIndex + 6);
        String basePath = baInputPath.substring(0, baInputPath.indexOf("meris-ba") - 1);
        return String.format("%s/sr-fr-default-nc-classic/%s/%s/%s/%s-%02d-*/CCI-Fire-*.nc", basePath, year, tile, year, year, month);
    }

    private static String lcYear(int year) {
        // 2002 -> 2000
        // 2003 - 2007 -> 2005
        // 2008 - 2012 -> 2010
        switch (year) {
            case 2002:
                return "2000";
            case 2003:
            case 2004:
            case 2005:
            case 2006:
            case 2007:
                return "2005";
            case 2008:
            case 2009:
            case 2010:
            case 2011:
            case 2012:
                return "2010";
        }
        throw new IllegalArgumentException("Illegal year: " + year);
    }

    private FileStatus[] getFileStatuses(HdfsInventoryService inventoryService,
                                         String inputPathPatterns,
                                         Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return inventoryService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
