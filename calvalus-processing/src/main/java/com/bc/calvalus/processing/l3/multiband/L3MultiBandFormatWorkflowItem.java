package com.bc.calvalus.processing.l3.multiband;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionBinIndex;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionSortingComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * A workflow item taking bins distributing them to reducers for formatting
 */
public class L3MultiBandFormatWorkflowItem extends HadoopWorkflowItem {

    public L3MultiBandFormatWorkflowItem(HadoopProcessingService processingService, String username, String jobName, Configuration jobConfig) {
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

        job.setMapperClass(L3MultiBandFormatMapper.class);
        job.setMapOutputKeyClass(L3MultiRegionBinIndex.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setPartitionerClass(L3MultiBandFormatPartitioner.class);
        job.setSortComparatorClass(L3MultiRegionSortingComparator.class);

        job.setReducerClass(L3MultiBandFormatReducer.class);

        String bandList = jobConfig.get("calvalus.output.bandList");
        int numBands = bandList.split(", *").length;
        job.setNumReduceTasks(jobConfig.getInt(JobConfigNames.CALVALUS_L3_REDUCERS, numBands));
        jobConfig.set(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, bandList);

        JobUtils.clearAndSetOutputDir(getOutputDir(), job, this);

        ProcessorFactory.installProcessorBundle(jobConfig);

        Map<String, String> metadata = ProcessingMetadata.read(FileInputFormat.getInputPaths(job)[0], jobConfig);
        ProcessingMetadata.metadata2Config(metadata, jobConfig, JobConfigNames.LEVEL3_METADATA_KEYS);
    }

}
