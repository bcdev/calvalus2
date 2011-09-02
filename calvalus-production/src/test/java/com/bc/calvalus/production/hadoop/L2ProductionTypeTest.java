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

package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestInventoryService;
import com.bc.calvalus.production.TestStagingService;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class L2ProductionTypeTest {

    private L2ProductionType productionType;

    @Before
    public void setUp() throws Exception {
        JobClient jobClient = new JobClient(new JobConf());
        productionType = new L2ProductionType(new TestInventoryService(),
                                              new HadoopProcessingService(jobClient),
                                              new TestStagingService());
    }

    @Test
    public void testCreateProductionWithoutDates() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                                    "inputPath", "MER_RR__1P/r03",
                                                                    "outputFormat", "NetCDF",
                                                                    "autoStaging", "true",
                                                                    "processorBundleName", "beam",
                                                                    "processorBundleVersion", "4.9-SNAPSHOT",
                                                                    "processorName", "BandMaths",
                                                                    "processorParameters", "<parameters/>",
                                                                    "minLon", "5",
                                                                    "maxLon", "25",
                                                                    "minLat", "50",
                                                                    "maxLat", "60"
        );

        Production production = productionType.createProduction(productionRequest);
        assertNotNull(production);
        assertEquals("Level 2 production using input path 'MER_RR__1P/r03' and L2 processor 'BandMaths'", production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + L2ProductionType.NAME + "_"));
        WorkflowItem workflow = production.getWorkflow();
        assertNotNull(workflow);
        WorkflowItem[] workflowItems = workflow.getItems();
        assertNotNull(workflowItems);
        assertEquals(0, workflowItems.length);
        assertTrue(workflow instanceof L2WorkflowItem);

        L2WorkflowItem l2WorkflowItem = (L2WorkflowItem) workflow;
        assertEquals(true, l2WorkflowItem.getOutputDir().contains("calvalus/outputs/ewa/"));
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        String inputFiles = l2WorkflowItem.getInputFiles();
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03", inputFiles);
    }

    @Test
    public void testCreateProductionWithMinMaxDates() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                                    "inputPath", "MER_RR__1P/r03/${yyyy}/${MM}/${dd}",
                                                                    "minDate", "2005-01-01",
                                                                    "maxDate", "2005-01-31",
                                                                    "outputFormat", "NetCDF",
                                                                    "autoStaging", "true",
                                                                    "processorBundleName", "beam",
                                                                    "processorBundleVersion", "4.9-SNAPSHOT",
                                                                    "processorName", "BandMaths",
                                                                    "processorParameters", "<!-- no params -->",
                                                                    "minLon", "5",
                                                                    "maxLon", "25",
                                                                    "minLat", "50",
                                                                    "maxLat", "60"
        );

        Production production = productionType.createProduction(productionRequest);
        assertNotNull(production);
        assertEquals("Level 2 production using input path 'MER_RR__1P/r03/${yyyy}/${MM}/${dd}' and L2 processor 'BandMaths'", production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + L2ProductionType.NAME + "_"));
        WorkflowItem workflow = production.getWorkflow();
        assertNotNull(workflow);
        WorkflowItem[] workflowItems = workflow.getItems();
        assertNotNull(workflowItems);
        assertEquals(0, workflowItems.length);
        assertTrue(workflow instanceof L2WorkflowItem);
        L2WorkflowItem l2WorkflowItem = (L2WorkflowItem) workflow;
        assertEquals(true, l2WorkflowItem.getOutputDir().contains("calvalus/outputs/ewa/"));
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        String inputFiles = l2WorkflowItem.getInputFiles();
        assertEquals("" +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/01," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/02," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/03," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/04," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/05," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/06," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/07," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/08," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/09," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/10," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/11," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/12," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/13," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/14," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/15," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/16," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/17," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/18," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/19," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/20," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/21," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/22," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/23," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/24," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/25," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/26," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/27," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/28," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/29," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/30," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/31",
                     inputFiles);
    }

    @Test
    public void testCreateProductionWithDatelist() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                                    "inputPath", "MER_RR__1P/r03/${yyyy}/${MM}/${dd}",
                                                                    "dateList", "2005-01-01 2005-01-15 2005-01-31",
                                                                    "outputFormat", "NetCDF",
                                                                    "autoStaging", "true",
                                                                    "processorBundleName", "beam",
                                                                    "processorBundleVersion", "4.9-SNAPSHOT",
                                                                    "processorName", "BandMaths",
                                                                    "processorParameters", "<!-- no params -->",
                                                                    "regionWKT", "POLYGON ((5 55, 25 50, 25 60, 5 60, 5 55))"
        );

        Production production = productionType.createProduction(productionRequest);
        assertNotNull(production);
        assertEquals("Level 2 production using input path 'MER_RR__1P/r03/${yyyy}/${MM}/${dd}' and L2 processor 'BandMaths'", production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + L2ProductionType.NAME + "_"));
        WorkflowItem workflow = production.getWorkflow();
        assertNotNull(workflow);
        WorkflowItem[] workflowItems = workflow.getItems();
        assertNotNull(workflowItems);
        assertEquals(0, workflowItems.length);
        assertTrue(workflow instanceof L2WorkflowItem);
        L2WorkflowItem l2WorkflowItem = (L2WorkflowItem) workflow;
        assertEquals(true, l2WorkflowItem.getOutputDir().contains("calvalus/outputs/ewa/"));
        assertEquals("POLYGON ((5 55, 25 50, 25 60, 5 60, 5 55))", l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        String inputFiles = l2WorkflowItem.getInputFiles();
        assertEquals("" +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/01," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/15," +
                             "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/31",
                     inputFiles);
    }

}
