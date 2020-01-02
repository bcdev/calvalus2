package com.bc.calvalus.processing.fire.format.grid.meris;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
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
public class MerisGridInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getFileStatuses(hdfsInventoryService, inputPathPatterns, conf);

        fileStatuses = CommonUtils.filterFileStatuses(fileStatuses);

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
            usedTiles.add(CommonUtils.getMerisTile(path.toString()));
        }
//        for (String tile : CommonUtils.getMissingTiles(usedTiles)) {
//            List<Path> filePaths = new ArrayList<>();
//            List<Long> fileLengths = new ArrayList<>();
        // dummy for BA input
//            filePaths.add(new Path("dummy"));
//            fileLengths.add(0L);
//
        // dummy for LC input
//            filePaths.add(new Path("dummy"));
//            fileLengths.add(0L);
//
        // srStatuses
//            FileStatus[] srPaths = getSrFileStatuses(conf.get("calvalus.year"), conf.get("calvalus.month"), tile, FileSystem.get(conf));
//            for (FileStatus status : srPaths) {
//                filePaths.add(status.getPath());
//                fileLengths.add(status.getLen());
//            }
//
//            boolean hasSrData = srPaths.length > 0;
//            if (hasSrData) {
//                splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
//                        fileLengths.stream().mapToLong(Long::longValue).toArray()));
//            }
//        }
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
        String lcYear = GridFormatUtils.lcYear(year);
        int tileIndex = baInputPath.indexOf("/BA_PIX_MER_") + "/BA_PIX_MER_".length();
        String tile = baInputPath.substring(tileIndex, tileIndex + 6);
        return baInputPath.substring(0, baInputPath.indexOf("meris-ba")) + "aux/lc/" + String.format("lc-%s-%s.nc", lcYear, tile);
    }

    static String getSrInputPathPattern(String baInputPath) {
        int yearIndex = baInputPath.indexOf("meris-ba/") + "meris-ba/".length();
        int monthIndex = baInputPath.indexOf("BA_PIX_MER") + 22;
        int year = Integer.parseInt(baInputPath.substring(yearIndex, yearIndex + 4));
        int month = Integer.parseInt(baInputPath.substring(monthIndex, monthIndex + 2));
        int tileIndex = baInputPath.indexOf("/BA_PIX_MER_") + "/BA_PIX_MER_".length();
        String tile = baInputPath.substring(tileIndex, tileIndex + 6);
        String basePath = baInputPath.substring(0, baInputPath.indexOf("meris-ba") - 1);
//        return String.format("%s/sr-fr-default-nc-classic/%s/%s/%s/%s-%02d-*/CCI-Fire-*.nc", basePath, year, tile, year, year, month);
        return String.format("%s/sr-fr-default/%s/l3-%s-%02d-*-fire-nc/CCI-Fire-*%s-%02d-*-%s*.nc", basePath, year, year, month, year, month, tile);
    }

    private FileStatus[] getFileStatuses(HdfsFileSystemService hdfsInventoryService,
                                         String inputPathPatterns,
                                         Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return hdfsInventoryService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
