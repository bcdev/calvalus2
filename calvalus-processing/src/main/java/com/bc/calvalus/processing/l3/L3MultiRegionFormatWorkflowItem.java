package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;

/**
 * A workflow item taking bins distributing them to reducers for formatting
 */
public class L3MultiRegionFormatWorkflowItem extends HadoopWorkflowItem {

    public L3MultiRegionFormatWorkflowItem(HadoopProcessingService processingService, String jobName, Configuration jobConfig) {
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
                {JobConfigNames.CALVALUS_L3_PARAMETERS, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        SequenceFileInputFormat.addInputPaths(job, getInputDir());
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(L3MultiRegionFormatMapper.class);
        job.setMapOutputKeyClass(L3RegionBinIndex.class);
        job.setMapOutputValueClass(L3TemporalBin.class);

        job.setPartitionerClass(L3MultiRegionFormatPartitioner.class);

        job.setReducerClass(L3MultiRegionFormatReducer.class);

        // hard code this to the number of regions
        // when the reducer supports the handling of multiple regions
        // this constrain cn be removed
        int numRegions = L3MultiRegionFormatConfig.get(jobConfig).getRegions().length;
        job.setNumReduceTasks(numRegions);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job);

        ProcessorFactory.installProcessorBundle(jobConfig);
    }

}
