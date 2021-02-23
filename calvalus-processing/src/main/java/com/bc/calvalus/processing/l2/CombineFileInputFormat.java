package com.bc.calvalus.processing.l2;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.FileSystemPathIteratorFactory;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.geodb.GeodbScanMapper;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
import java.util.Set;

/**
 * @author thomas
 */
public class CombineFileInputFormat extends PatternBasedInputFormat {
    /**
     * Creates a single split from a given pattern
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>(1);
        List<InputSplit> rawSplits = super.getSplits(context);
        if (rawSplits.size() > 0 || context.getConfiguration().getBoolean("calvalus.generateEmptyAggregate", false)) {
            Path[] filePaths = new Path[rawSplits.size()];
            long[] fileLengths = new long[rawSplits.size()];
            for (int i = 0; i < rawSplits.size(); ++i) {
                ProductSplit split = (ProductSplit) rawSplits.get(i);
                filePaths[i] = split.getPath();
                fileLengths[i] = split.getLength();
            }
            splits.add(new CombineFileSplit(filePaths, fileLengths));
            CalvalusLogger.getLogger().info(String.format("Combined %d files into 1 split.", rawSplits.size()));
        } else {
            CalvalusLogger.getLogger().info(String.format("Combined %d files into 0 splits.", rawSplits.size()));
        }
        return splits;
    }

    /**
     * Creates a single split from a given pattern
     */
/*
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String inputPathPattern = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);

        List<InputSplit> splits = new ArrayList<>(1);
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsFileSystemService = new HdfsFileSystemService(jobClientsMap);
        List<String> inputPatterns = new InputPathResolver().resolve(inputPathPattern);
        RemoteIterator<LocatedFileStatus> fileStatusIt = getFileStatuses(hdfsFileSystemService, inputPatterns, conf, null);
        addSplit(fileStatusIt, splits);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private void addSplit(RemoteIterator<LocatedFileStatus> fileStatuses, List<InputSplit> splits) throws IOException {
        List<Path> filePaths = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();
        while (fileStatuses.hasNext()) {
            LocatedFileStatus fileStatus = fileStatuses.next();
            Path path = fileStatus.getPath();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());
        }
        CombineFileSplit combineFileSplit = new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                fileLengths.stream().mapToLong(Long::longValue).toArray());
        splits.add(combineFileSplit);
    }


    protected RemoteIterator<LocatedFileStatus> getFileStatuses(HdfsFileSystemService fileSystemService,
                                                                List<String> inputPatterns,
                                                                Configuration conf,
                                                                Set<String> existingPathes) throws IOException {
        FileSystemPathIteratorFactory.FileStatusFilter extraFilter = null;
        if (existingPathes != null && existingPathes.size() > 0) {
            extraFilter = fileStatus -> {
                String dbPath = GeodbScanMapper.getDBPath(fileStatus.getPath(), conf);
                return !existingPathes.contains(dbPath);
            };
        }
        return fileSystemService.globFileStatusIterator(inputPatterns, conf, extraFilter, false, true);  // TODO verify the two last params
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NoRecordReader();
    }
*/
}
