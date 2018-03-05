package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class LcSubsetterInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("s2-files.txt")));
        String fileSubPath;
        List<InputSplit> splits = new ArrayList<>(4000);
        FileSystem fileSystem = FileSystem.get(conf);
        while ((fileSubPath = bufferedReader.readLine()) != null) {
            String tile = fileSubPath.split("/")[0];
            String targetFilename = "lc-2010-" + tile + ".nc";
            Path targetPath = new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc4s2-from-s2/", targetFilename);
            if ((fileSystem.exists(targetPath))) {
                continue;
            }
            String filePath = "hdfs://calvalus/calvalus/projects/fire/s2-ba/" + fileSubPath;
            Path f = new Path(filePath);
            if (fileSystem.exists(f)) {
                addSplit(fileSystem.getFileStatus(f), splits);
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

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
