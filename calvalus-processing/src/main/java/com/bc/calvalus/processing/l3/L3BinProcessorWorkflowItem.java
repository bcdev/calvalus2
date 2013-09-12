package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.l3.cellstream.CellInputFormat;
import com.bc.calvalus.processing.l3.cellstream.CellOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * A workflow item taking bins and further aggregating them
 */
public class L3BinProcessorWorkflowItem extends HadoopWorkflowItem {

    public L3BinProcessorWorkflowItem(HadoopProcessingService processingService, String jobName, Configuration jobConfig) {
        super(processingService, jobName, jobConfig);
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
                {JobConfigNames.CALVALUS_L3_PARAMETERS_FIRST, null},
                {JobConfigNames.CALVALUS_L3_PARAMETERS, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MIN_DATE, NO_DEFAULT},
                {JobConfigNames.CALVALUS_MAX_DATE, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        FileInputFormat.addInputPaths(job, getInputDir());
        job.setInputFormatClass(CellInputFormat.class);

        job.setMapperClass(L3BinProcessorMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(L3SpatialBin.class);

        job.setPartitionerClass(L3Partitioner.class);

        job.setReducerClass(L3Reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(L3TemporalBin.class);
        job.setNumReduceTasks(jobConfig.getInt(JobConfigNames.CALVALUS_L3_REDUCERS, 8));

        JobUtils.clearAndSetOutputDir(getOutputDir(), job);
        job.setOutputFormatClass(CellOutputFormat.class);

        ProcessorFactory.installProcessorBundle(jobConfig);
    }

}
