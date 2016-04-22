package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
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
import java.util.Collections;
import java.util.List;

/**
 * @author thomas
 * @author marcop
 */
public class FireGridInputFormat extends InputFormat {

    private static final int MAX_X_TILE = 34;
    private static final int MAX_Y_TILE = 17;
    private HdfsInventoryService hdfsInventoryService;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        hdfsInventoryService = new HdfsInventoryService(jobClientsMap, "eodata");

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getFileStatuses(hdfsInventoryService, inputPathPatterns, conf);

        createSplits(fileStatuses, splits, conf);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    protected void createSplits(FileStatus[] fileStatuses,
                              List<InputSplit> splits,
                              Configuration conf) throws IOException {
        for (FileStatus fileStatus : fileStatuses) {
            List<Path> filePaths = new ArrayList<>(9);
            List<Long> fileLengths = new ArrayList<>(9);
            Path path = fileStatus.getPath();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());

            int[] tileIndices = getTileIndices(path.getName());
            for (int y = Math.max(0, tileIndices[0] - 1); y <= Math.min(tileIndices[0] + 1, MAX_Y_TILE); y++) {
                for (int x = Math.max(0, tileIndices[1] - 1); x <= Math.min(tileIndices[1] + 1, MAX_X_TILE); x++) {
                    if (y == tileIndices[0] && x == tileIndices[1]) {
                        // center product, already inserted
                        continue;
                    }
                    String neighbourName = getNeighbourName(path.getName(), x, y);
                    String neighbourPath = fileStatus.getPath().toString().replace(path.getName(), neighbourName);
                    FileStatus[] neighbours = hdfsInventoryService.globFileStatuses(Collections.singletonList(neighbourPath), conf);
                    if (neighbours.length > 0) {
                        FileStatus neighbour = neighbours[0];
                        if (neighbour != null) {
                            filePaths.add(neighbour.getPath());
                            fileLengths.add(neighbour.getLen());
                        }
                    }
                }
            }
            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
        }
    }

    static String getNeighbourName(String pathName, int x, int y) {
        String tile = String.format("v%02dh%02d", y, x);
        return pathName.replaceAll("v..h..", tile);
    }

    protected FileStatus[] getFileStatuses(HdfsInventoryService inventoryService,
                                            String inputPathPatterns,
                                            Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return inventoryService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }

    static int[] getTileIndices(String filename) {
//      sample filename: BA_PIX_MER_v07h11_200806_v4.0.tif
        String tile = filename.substring(12, 18);
        int[] indices = new int[2];
        indices[0] = Integer.parseInt(tile.substring(0, 2));
        indices[1] = Integer.parseInt(tile.substring(3, 5));
        return indices;
    }

}
