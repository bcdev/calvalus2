package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.hadoop.ProgressSplit;
import com.google.gson.Gson;
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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * @author thomas
 */
public class ModisGridInputFormat extends InputFormat {

    private static String[] expectedInputTiles = new String[]{
            "h12v02", "h12v09", "h13v03", "h13v12", "h14v09", "h16v02", "h17v03", "h18v02", "h18v08", "h19v06", "h19v12", "h20v06", "h20v12", "h21v06", "h22v01", "h22v07", "h23v03", "h23v11", "h24v07", "h25v07", "h26v06", "h27v06", "h28v04", "h28v11", "h29v08", "h30v07", "h30v13", "h31v13", "h33v10",
            "h12v03", "h12v10", "h13v04", "h13v13", "h14v10", "h16v05", "h17v04", "h18v03", "h18v09", "h19v07", "h20v01", "h20v07", "h21v01", "h21v07", "h22v02", "h22v08", "h23v04", "h24v02", "h25v02", "h25v08", "h26v07", "h27v07", "h28v05", "h28v12", "h29v09", "h30v08", "h31v07", "h32v07", "h33v11",
            "h12v04", "h12v11", "h13v08", "h13v14", "h14v11", "h16v06", "h17v05", "h18v04", "h19v02", "h19v08", "h20v02", "h20v08", "h21v02", "h21v08", "h22v03", "h22v09", "h23v05", "h24v03", "h25v03", "h26v02", "h26v08", "h27v08", "h28v06", "h28v13", "h29v10", "h30v09", "h31v09", "h32v09", "h34v10",
            "h12v05", "h12v12", "h13v09", "h14v02", "h14v14", "h16v07", "h17v06", "h18v05", "h19v03", "h19v09", "h20v03", "h20v09", "h21v03", "h21v09", "h22v04", "h22v10", "h23v06", "h24v04", "h25v04", "h26v03", "h27v03", "h27v09", "h28v07", "h29v05", "h29v11", "h30v10", "h31v10", "h32v10", "h35v10",
            "h12v07", "h12v13", "h13v10", "h14v03", "h15v02", "h16v08", "h17v07", "h18v06", "h19v04", "h19v10", "h20v04", "h20v10", "h21v04", "h21v10", "h22v05", "h22v11", "h23v07", "h24v05", "h25v05", "h26v04", "h27v04", "h27v11", "h28v08", "h29v06", "h29v12", "h30v11", "h31v11", "h32v12",
            "h12v08", "h13v02", "h13v11", "h14v04", "h15v07", "h17v02", "h17v08", "h18v07", "h19v05", "h19v11", "h20v05", "h20v11", "h21v05", "h21v11", "h22v06", "h23v02", "h23v08", "h24v06", "h25v06", "h26v05", "h27v05", "h27v12", "h28v09", "h29v07", "h29v13", "h30v12", "h31v12", "h33v09"
    };

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
        tilesLut = getTilesLut(modisTilesFile);
        List<InputSplit> splits = new ArrayList<>(tilesLut.size());

        for (String targetCell : tilesLut.keySet()) {

            List<FileStatus> fileStatuses = new ArrayList<>();
            Set<String> inputTiles = tilesLut.get(targetCell);
            List<String> tilesWithBurnedData = new ArrayList<>();
            for (String inputTile : inputTiles) {
                String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/modis-ba/" + year + "/" + month + "/burned_" + year + "_" + singleMonth + "_" + inputTile + ".nc";
                FileStatus[] elements = getFileStatuses(inputPathPattern, conf);
                if (elements.length > 0) {
                    tilesWithBurnedData.add(inputTile);
                } else {
                    if (Arrays.asList(expectedInputTiles).contains(inputTile)) {
                        throw new IllegalStateException("Missing file: burned_" + year + "_" + singleMonth + "_" + inputTile + ".nc");
                    }
                }
                Collections.addAll(fileStatuses, elements);
            }
            addSplit(fileStatuses.toArray(new FileStatus[0]), splits, targetCell, inputTiles, tilesWithBurnedData);
        }

        if (Boolean.parseBoolean(conf.get("calvalus.onlyCheck", "true"))) {
            throw new IllegalStateException("Only performed the check, produced no input splits.");
        }

        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void addSplit(FileStatus[] fileStatuses, List<InputSplit> splits, String targetCell, Set<String> inputTiles, List<String> tilesWithBurnedData) throws IOException {
        List<Path> filePaths = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();
        for (String inputTile : inputTiles) {
            if (tilesWithBurnedData.contains(inputTile)) {
                for (FileStatus fileStatus : fileStatuses) {
                    Path path = fileStatus.getPath();
                    if (path.getName().contains(inputTile)) {
                        filePaths.add(path);
                        fileLengths.add(fileStatus.getLen());
                    }
                }
            } else {
                filePaths.add(new Path("dummyburned_year_month_" + inputTile));
                fileLengths.add(0L);
            }
        }
        filePaths.add(new Path(targetCell));
        fileLengths.add(0L);
        CombineFileSplit combineFileSplit = new ProgressableCombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
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

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);
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

    public static class ProgressableCombineFileSplit extends CombineFileSplit implements ProgressSplit {

        private float progress;

        /**
         * For deserialize only
         */
        public ProgressableCombineFileSplit() {
        }

        ProgressableCombineFileSplit(Path[] files, long[] lengths) {
            super(files, lengths);
        }

        @Override
        public void setProgress(float progress) {
            this.progress = progress;
        }

        @Override
        public float getProgress() {
            return progress;
        }
    }
}
