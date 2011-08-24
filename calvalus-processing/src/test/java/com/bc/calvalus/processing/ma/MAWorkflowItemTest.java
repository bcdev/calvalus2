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

import com.bc.calvalus.processing.JobConfNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import static org.junit.Assert.*;
/**
 * @author MarcoZ
 */
public class MAWorkflowItemTest {
    @Test
    public void testCreateJob() throws Exception {
        HadoopProcessingService hadoopProcessingService = new HadoopProcessingService(new JobClient(new JobConf()));
        MAConfig maConfig1 = new MAConfig();
        maConfig1.setRecordSourceSpiClassName(TestRecordSourceSpi.class.getName());

        String jobName = "MaTestJob";
        String[] inputFiles = new String[]{"file1", "file2"};
        String outputDir = "outputOfMaJob";

        MAWorkflowItem maWorkflowItem = new MAWorkflowItem(hadoopProcessingService,
                                                           jobName,
                                                           "",
                                                           "",
                                                           "",
                                                           null,
                                                           inputFiles,
                                                           "HADOOP-STREAMING",
                                                           outputDir,
                                                           maConfig1,
                                                           "",
                                                           "");
        assertNotNull(maWorkflowItem);
        assertEquals(outputDir, maWorkflowItem.getOutputDir());
        Job job = new Job();
        maWorkflowItem.configureJob(job);
        assertNotNull(job);

        assertSame(MAMapper.class, job.getMapperClass());
        assertSame(Text.class, job.getMapOutputKeyClass());
        assertSame(RecordWritable.class, job.getMapOutputValueClass());

        Configuration configuration = job.getConfiguration();
        String xml = configuration.get(JobConfNames.CALVALUS_MA_PARAMETERS);
        assertTrue(xml.startsWith("<parameters>"));
        assertTrue(xml.endsWith("</parameters>"));
        assertTrue(xml.contains(TestRecordSourceSpi.class.getName()));

        assertEquals("file1,file2", configuration.get(JobConfNames.CALVALUS_INPUT));

        assertSame(TextOutputFormat.class, job.getOutputFormatClass());
        assertEquals(outputDir, FileOutputFormat.getOutputPath(job).getName());

    }
}
