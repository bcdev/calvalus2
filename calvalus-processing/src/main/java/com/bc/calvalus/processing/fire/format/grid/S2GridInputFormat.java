package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
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
import java.util.List;
import java.util.Properties;

/**
 * @author thomas
 */
public class S2GridInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String tile = conf.get("calvalus.tile");
        String year = conf.get("calvalus.tile");
        String month = conf.get("calvalus.tile");
        Properties tiles = new Properties();
        tiles.load(getClass().getResourceAsStream("areas-tiles.properties"));
        String tilePattern = tiles.get(tile).toString();

        String inputPathPatterns = "hdfs://calvalus/calvalus/projects/fire/s2-ba/" + tilePattern + "/BA-.*" + year + month + ".*nc";

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService hdfsInventoryService = new HdfsInventoryService(jobClientsMap, "eodata");

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getFileStatuses(hdfsInventoryService, inputPathPatterns, conf);

        createSplits(fileStatuses, splits, conf, tile);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void createSplits(FileStatus[] fileStatuses,
                              List<InputSplit> splits, Configuration conf, String tile) throws IOException {

        for (FileStatus fileStatus : fileStatuses) {
            List<Path> filePaths = new ArrayList<>();
            List<Long> fileLengths = new ArrayList<>();
            Path path = fileStatus.getPath();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());
            String lcInputPath = "hdfs://calvalus/calvalus/projects/fire/aux/lc/" + String.format("lc-%s-%s.nc", "2005", tile);
            FileStatus lcPath = path.getFileSystem(conf).getFileStatus(new Path(lcInputPath));
            filePaths.add(lcPath.getPath());
            fileLengths.add(lcPath.getLen());

            splits.add(new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                    fileLengths.stream().mapToLong(Long::longValue).toArray()));
        }
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
