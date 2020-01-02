package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.grid.s2.GeoLutInputFormat;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.fs.Path;
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

@SuppressWarnings("unused")
public class Sen2CorMergerInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>(3000);
        String configuredTiles = context.getConfiguration().get("calvalus.tiles");
        if (configuredTiles == null) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(GeoLutInputFormat.class.getResourceAsStream("s2-tiles.txt")));
            String tile;
            while ((tile = bufferedReader.readLine()) != null) {
                List<Path> filePaths = new ArrayList<>();
                List<Long> fileLengths = new ArrayList<>();
                filePaths.add(new Path(tile));
                fileLengths.add(0L);
                CombineFileSplit combineFileSplit = new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                        fileLengths.stream().mapToLong(Long::longValue).toArray());
                splits.add(combineFileSplit);
            }
        } else {
            String[] tiles = configuredTiles.split(",");
            for (String tile : tiles) {
                List<Path> filePaths = new ArrayList<>();
                List<Long> fileLengths = new ArrayList<>();
                filePaths.add(new Path(tile));
                fileLengths.add(0L);
                CombineFileSplit combineFileSplit = new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                        fileLengths.stream().mapToLong(Long::longValue).toArray());
                splits.add(combineFileSplit);
            }
        }

        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
