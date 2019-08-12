package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class GeoLutInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        int minTilePrefix = Integer.parseInt(conf.get("calvalus.minTilePrefix"));
        int maxTilePrefix = Integer.parseInt(conf.get("calvalus.maxTilePrefix"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("s2-tiles.txt")));
        String line;
        List<InputSplit> splits = new ArrayList<>(3000);
        while ((line = bufferedReader.readLine()) != null) {
            int tilePrefix = Integer.parseInt(line.substring(0, 2));
            if (tilePrefix > maxTilePrefix || tilePrefix < minTilePrefix) {
                continue;
            }
            CalvalusLogger.getLogger().info("Searching for file containing tile " + line + "...");
            boolean notFound = true;
            for (int year : new int[]{2015, 2016}) {
                for (String month : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",}) {
                    if (notFound) {
                        String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/s2-pre/" + year + "/" + month + "/.*/.*T" + line + ".tif";
                        FileStatus[] fileStatuses = getFileStatuses(inputPathPattern, conf);
                        if (fileStatuses.length == 0) {
                            continue;
                        }
                        FileStatus fileStatus = fileStatuses[0];
                        addSplit(fileStatus, splits);
                        notFound = false;
                        CalvalusLogger.getLogger().info("...found.");
                    }
                }
            }
        }

        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void addSplit(FileStatus fileStatus, List<InputSplit> splits) throws IOException {
        Path path = fileStatus.getPath();
        CombineFileSplit split = new CombineFileSplit(new Path[]{path}, new long[]{fileStatus.getLen()});
        splits.add(split);
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
}
