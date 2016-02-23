package com.bc.calvalus.processing.l3.multiregion;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * A workflow item taking bins distributing them to reducers for formatting
 */
public class L3MultiRegionFormatWorkflowItem extends HadoopWorkflowItem {

    public L3MultiRegionFormatWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
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
                {JobConfigNames.CALVALUS_OUTPUT_DIR, NO_DEFAULT}
        };
    }

    @Override
    protected void configureJob(Job job) throws IOException {
        Configuration jobConfig = job.getConfiguration();

        FileInputFormat.addInputPaths(job, getInputDir());
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(L3MultiRegionFormatMapper.class);
        job.setMapOutputKeyClass(L3MultiRegionBinIndex.class);
        job.setMapOutputValueClass(L3MultiRegionTemporalBin.class);

        job.setPartitionerClass(L3MultiRegionFormatPartitioner.class);
        job.setSortComparatorClass(L3MultiRegionSortingComparator.class);
        job.setGroupingComparatorClass(L3MultiRegionGroupingComparator.class);

        job.setReducerClass(L3MultiRegionFormatReducer.class);

        int numRegions = L3MultiRegionFormatConfig.get(jobConfig).getRegions().length;
        job.setNumReduceTasks(jobConfig.getInt(JobConfigNames.CALVALUS_L3_REDUCERS, numRegions));

        JobUtils.clearAndSetOutputDir(getOutputDir(), job, this);

        Map<String, String> metadata = ProcessingMetadata.read(FileInputFormat.getInputPaths(job)[0], jobConfig);
        ProcessingMetadata.metadata2Config(metadata, jobConfig, JobConfigNames.LEVEL3_METADATA_KEYS);
    }

}
