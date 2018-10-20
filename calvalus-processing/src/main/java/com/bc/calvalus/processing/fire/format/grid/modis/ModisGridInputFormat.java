package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.hadoop.ProgressSplit;
import org.apache.hadoop.fs.Path;
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
 */
public class ModisGridInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();

        for (int y = 0; y < 20; y++) {
            for (int x = 0; x < 40; x++) {
                addSplit(x * 36 + "," + y * 36, splits);
            }
        }

        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void addSplit(String targetCell, List<InputSplit> splits) {
        List<Path> filePaths = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();
        filePaths.add(new Path(targetCell));
        fileLengths.add(0L);
        CombineFileSplit combineFileSplit = new ProgressableCombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                fileLengths.stream().mapToLong(Long::longValue).toArray());
        splits.add(combineFileSplit);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NoRecordReader();
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
