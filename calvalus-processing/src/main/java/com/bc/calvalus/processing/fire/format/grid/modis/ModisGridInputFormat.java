package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author thomas
 */
public class ModisGridInputFormat extends InputFormat {

    /**
     * Creates splits for each target grid cell, that combine all BA tiles ba and LC tiles lc like this:
     * <p>
     * InputSplit[0] = ba (of some tile t)
     * InputSplit[1] = lc (of tile t)
     * InputSplit[2] = ba (of some tile t2)
     * InputSplit[3] = lc (of tile t2)
     *
     * @param context
     * @return
     * @throws IOException
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String year = conf.get("calvalus.year");
        String month = conf.get("calvalus.month");
        File modisTilesFile = new File("modis-tiles-lut.txt");
        CalvalusProductIO.copyFileToLocal(new Path("/calvalus/projects/fire/aux/modis-tiles/modis-tiles-lut.txt"), modisTilesFile, conf);
        GeoLutCreator.Result tilesLut = getTilesLut(modisTilesFile);
        List<InputSplit> splits = new ArrayList<>(tilesLut.size());

        for (String targetTile : tilesLut.keySet()) {
            List<FileStatus> fileStatuses = new ArrayList<>();
            SortedSet<String> inputTiles = new TreeSet<>(tilesLut.get(targetTile));
            for (String inputTile : inputTiles) {
                String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/modis-ba/" + inputTile + "/BA-.*" + year + month + ".*nc";
                Collections.addAll(fileStatuses, getFileStatuses(inputPathPattern, conf));
            }
            addSplit(fileStatuses.toArray(new FileStatus[0]), splits, conf, GridFormatUtils.lcYear(Integer.parseInt(year)), targetTile, inputTiles);
        }
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void addSplit(FileStatus[] fileStatuses, List<InputSplit> splits, Configuration conf, String lcYear, String targetTile, SortedSet<String> inputTileSet) throws IOException {
        if (fileStatuses.length != inputTileSet.size()) {
            throw new IllegalStateException("fileStatuses.length != inputTileSet.size()");
        }
        List<Path> filePaths = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();
        String[] inputTiles = inputTileSet.toArray(new String[0]);
        for (int i = 0; i < fileStatuses.length; i++) {
            FileStatus fileStatus = fileStatuses[i];
            Path path = fileStatus.getPath();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());
            String lcTile = inputTiles[i];
            String lcInputPath = "hdfs://calvalus/calvalus/projects/fire/aux/modis-lc/" + String.format("%s-%s.nc", lcTile, lcYear);
            FileStatus lcPath = FileSystem.get(conf).getFileStatus(new Path(lcInputPath));
            filePaths.add(lcPath.getPath());
            fileLengths.add(lcPath.getLen());
        }
        filePaths.add(new Path(targetTile));
        fileLengths.add(0L);
        CombineFileSplit combineFileSplit = new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                fileLengths.stream().mapToLong(Long::longValue).toArray());
        splits.add(combineFileSplit);
    }

    static GeoLutCreator.Result getTilesLut(File modisTilesFile) {
        Gson gson = new Gson();
        GeoLutCreator.Result result;
        try (FileReader fileReader = new FileReader(modisTilesFile)) {
            result = gson.fromJson(fileReader, GeoLutCreator.Result.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return result;

    }

    private FileStatus[] getFileStatuses(String inputPathPatterns,
                                         Configuration conf) throws IOException {

        HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(conf, "eodata");
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return hdfsInventoryService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
