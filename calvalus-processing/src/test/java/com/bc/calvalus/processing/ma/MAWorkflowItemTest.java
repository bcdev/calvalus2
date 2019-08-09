/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.ma;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.JobClientsMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author MarcoZ
 */
public class MAWorkflowItemTest {

    @Test
    public void testCreateJob() throws Exception {
        HadoopProcessingService processingService = new HadoopProcessingService(new JobClientsMap(new JobConf()));

        MAConfig maConfig = new MAConfig();
        maConfig.setRecordSourceSpiClassName(TestRecordSourceSpi.class.getName());

        String jobName = "MaTestJob";
        Configuration jobConfig = processingService.createJobConfig("testuser");
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, "file1,file2");
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_FORMAT, "HADOOP-STREAMING");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, "out_004");
        jobConfig.set(JobConfigNames.CALVALUS_MA_PARAMETERS, maConfig.toXml());
        MAWorkflowItem maWorkflowItem = new MAWorkflowItem(processingService, "testuser", jobName, jobConfig);

        Job job = Job.getInstance(jobConfig);
        maWorkflowItem.configureJob(job);

        assertSame(MAMapper.class, job.getMapperClass());
        assertSame(Text.class, job.getMapOutputKeyClass());
        assertSame(RecordWritable.class, job.getMapOutputValueClass());

        assertSame(SequenceFileOutputFormat.class, job.getOutputFormatClass());
        assertEquals("out_004", FileOutputFormat.getOutputPath(job).getName());

    }
}
