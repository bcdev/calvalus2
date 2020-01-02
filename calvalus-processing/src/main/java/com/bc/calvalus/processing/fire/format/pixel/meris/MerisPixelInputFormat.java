package com.bc.calvalus.processing.fire.format.pixel.meris;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.MerisStrategy;
import com.bc.calvalus.processing.fire.format.PixelProductArea;
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

import static com.bc.calvalus.processing.fire.format.CommonUtils.getMerisTile;

/**
 * @author thomas
 */
public class MerisPixelInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        PixelProductArea area = new MerisStrategy().getArea(conf.get("area"));
        String inputPathPattern = getInputPathPattern(context.getConfiguration().get("calvalus.year"), context.getConfiguration().get("calvalus.month"), area);
        CalvalusLogger.getLogger().info("Input path pattern = " + inputPathPattern);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsFileSystemService = new HdfsFileSystemService(jobClientsMap);

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getFileStatuses(hdfsFileSystemService, inputPathPattern, conf);

        fileStatuses = CommonUtils.filterFileStatuses(fileStatuses);

        createSplits(fileStatuses, splits, conf, hdfsFileSystemService);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    static String getInputPathPattern(String year, String month, PixelProductArea area) {
        StringBuilder groupsForArea = new StringBuilder();
        int firstTileV = area.top / 10;
        int firstTileH = area.left / 10;
        int lastTileV = area.bottom / 10;
        int lastTileH = area.right / 10;
        for (int tileV = firstTileV; tileV <= lastTileV; tileV++) {
            for (int tileH = firstTileH; tileH <= lastTileH; tileH++) {
                String tile = String.format("v%02dh%02d|", tileV, tileH);
                groupsForArea.append(tile);
            }
        }
        return String.format("hdfs://calvalus/calvalus/projects/fire/meris-ba/%s/.*(%s).*%s%s.*tif", year, groupsForArea.substring(0, groupsForArea.length() - 1), year, month);
    }

    static String getLcInputPathPattern(String year, PixelProductArea area) {
        StringBuilder groupsForArea = new StringBuilder();
        int firstTileV = area.top / 10;
        int firstTileH = area.left / 10;
        int lastTileV = area.bottom / 10;
        int lastTileH = area.right / 10;
        for (int tileV = firstTileV; tileV <= lastTileV; tileV++) {
            for (int tileH = firstTileH; tileH <= lastTileH; tileH++) {
                String tile = String.format("v%02dh%02d|", tileV, tileH);
                groupsForArea.append(tile);
            }
        }
        return String.format("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-%s-(%s).*nc", CommonUtils.lcYear(Integer.parseInt(year)), groupsForArea.substring(0, groupsForArea.length() - 1));
    }

    private void createSplits(FileStatus[] fileStatuses, List<InputSplit> splits, Configuration conf, HdfsFileSystemService hdfsFileSystemService) throws IOException {
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
            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
            usedTiles.add(getMerisTile(path.toString()));
        }

        String year = conf.get("calvalus.year");
        PixelProductArea area = new MerisStrategy().getArea(conf.get("calvalus.area"));
        String lcInputPathPattern = getLcInputPathPattern(year, area);
        for (String usedTile : usedTiles) {
            lcInputPathPattern = lcInputPathPattern.replace(usedTile + "|", "").replace(usedTile, "");
        }
        lcInputPathPattern = lcInputPathPattern.replace("|)", ")");
        CalvalusLogger.getLogger().info("LC input path pattern = " + lcInputPathPattern);
        FileStatus[] lcFileStatuses = getFileStatuses(hdfsFileSystemService, lcInputPathPattern, conf);
        for (FileStatus lcFileStatus : lcFileStatuses) {
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            // dummy for BA input
            filePaths.add(new Path("dummy"));
            fileLengths.add(0L);
            filePaths.add(lcFileStatus.getPath());
            fileLengths.add(lcFileStatus.getLen());
            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
        }
    }

    private FileStatus getLcFileStatus(Path path, FileSystem fileSystem) throws IOException {
        String baInputPath = path.toString(); // hdfs://calvalus/calvalus/projects/fire/meris-ba/$year/BA_PIX_MER_$tile_$year$month_v4.0.tif
        String lcInputPath = getLcInputPath(baInputPath);
        return fileSystem.getFileStatus(new Path(lcInputPath));
    }

    private static String getLcInputPath(String baInputPath) {
        int yearIndex = baInputPath.indexOf("meris-ba/") + "meris-ba/".length();
        int year = Integer.parseInt(baInputPath.substring(yearIndex, yearIndex + 4));
        String lcYear = CommonUtils.lcYear(year);
        String tile = getMerisTile(baInputPath);
        return baInputPath.substring(0, baInputPath.indexOf("meris-ba")) + "aux/lc/" + String.format("lc-%s-%s.nc", lcYear, tile);
    }

    private FileStatus[] getFileStatuses(HdfsFileSystemService hdfsFileSystemService,
                                         String inputPathPatterns,
                                         Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return hdfsFileSystemService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
