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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
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
        String singleMonth = Integer.toString(Integer.parseInt(conf.get("calvalus.month")));
        TileLut tilesLut;
        File modisTilesFile = new File("modis-tiles-lut.txt");
        try {
            CalvalusProductIO.copyFileToLocal(new Path("/calvalus/projects/fire/aux/modis-tiles/modis-tiles-lut.txt"), modisTilesFile, conf);
            tilesLut = getTilesLut(modisTilesFile);
        } finally {
            Files.delete(Paths.get(modisTilesFile.toURI()));
        }
        List<InputSplit> splits = new ArrayList<>(tilesLut.size());

        for (String targetCell : tilesLut.keySet()) {
            List<FileStatus> fileStatuses = new ArrayList<>();
            SortedSet<String> inputTiles = new TreeSet<>(tilesLut.get(targetCell));
            for (String inputTile : inputTiles) {
                String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/modis-ba/" + year + "/" + month + "/burned_" + year + "_" + singleMonth + "_" + inputTile + ".nc";
                Collections.addAll(fileStatuses, getFileStatuses(inputPathPattern, conf));
            }
            if (!fileStatuses.isEmpty()) {
                addSplit(fileStatuses.toArray(new FileStatus[0]), splits, conf, GridFormatUtils.lcYear(Integer.parseInt(year)), targetCell, inputTiles);
            }
        }

        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void addSplit(FileStatus[] fileStatuses, List<InputSplit> splits, Configuration conf, String lcYear, String targetCell, SortedSet<String> inputTileSet) throws IOException {
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
        filePaths.add(new Path(targetCell));
        fileLengths.add(0L);
        CombineFileSplit combineFileSplit = new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                fileLengths.stream().mapToLong(Long::longValue).toArray());
        splits.add(combineFileSplit);
    }

    static TileLut getTilesLut(File modisTilesFile) {
        Gson gson = new Gson();
        TileLut tileLut;
        try (FileReader fileReader = new FileReader(modisTilesFile)) {
            tileLut = gson.fromJson(fileReader, TileLut.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return tileLut;

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

    public static class TileLut extends HashMap<String, Set<String>> {
        // only needed for GSON to serialise
    }
}
