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
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.*;
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
                                                                    "processorParameters", "<!-- no params -->",
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

        String[] inputFiles = l2WorkflowItem.getInputFiles();
        assertNotNull(inputFiles);
        assertEquals(1, inputFiles.length);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03", inputFiles[0]);
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

        String[] inputFiles = l2WorkflowItem.getInputFiles();
        assertNotNull(inputFiles);
        assertEquals(31, inputFiles.length);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/01", inputFiles[0]);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/02", inputFiles[1]);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/31", inputFiles[30]);
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

        String[] inputFiles = l2WorkflowItem.getInputFiles();
        assertNotNull(inputFiles);
        assertEquals(3, inputFiles.length);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/01", inputFiles[0]);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/15", inputFiles[1]);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/31", inputFiles[2]);
    }

}
