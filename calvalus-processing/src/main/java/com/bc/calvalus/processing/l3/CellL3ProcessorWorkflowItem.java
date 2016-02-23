package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.l3.cellstream.CellInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * A workflow item taking bins and further aggregating them
 */
public class CellL3ProcessorWorkflowItem extends HadoopWorkflowItem {

    public CellL3ProcessorWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
        super(processingService, username, jobName, jobConfig);
    }

    public String getInputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_INPUT_DIR);
    }

    @Override
    public String getOutputDir() {
        return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR);
    }

    @Override
    protected String[][] getJobConfigDefaults() {
        return new String[][]{
                {JobConfigNames.CALVALUS_INPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT},
                {JobConfigNames.CALVALUS_CELL_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MIN_DATE, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MAX_DATE, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        FileInputFormat.addInputPaths(job, getInputDir());
        job.setInputFormatClass(CellInputFormat.class);

        job.setMapperClass(CellL3ProcessorMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(L3SpatialBin.class);

        job.setPartitionerClass(L3Partitioner.class);

        job.setReducerClass(L3Reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(L3TemporalBin.class);
        job.setNumReduceTasks(jobConfig.getInt(JobConfigNames.CALVALUS_L3_REDUCERS, 8));

        JobUtils.clearAndSetOutputDir(getOutputDir(), job, this);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        CellInputFormat cellInputFormat = new CellInputFormat();
        Path inputDirectory = cellInputFormat.getFirstInputDirectory(job);
        if (inputDirectory != null) {
            Map<String, String> metadata = ProcessingMetadata.read(inputDirectory, jobConfig);
            String[] coreL3Keys = {
                    JobConfigNames.CALVALUS_REGION_GEOMETRY,
                    JobConfigNames.CALVALUS_INPUT_REGION_NAME,
                    JobConfigNames.CALVALUS_L3_PARAMETERS
            };
            ProcessingMetadata.metadata2Config(metadata, jobConfig, coreL3Keys);
        }
    }
}