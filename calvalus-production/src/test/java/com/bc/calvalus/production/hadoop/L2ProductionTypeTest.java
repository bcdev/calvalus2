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
import com.bc.calvalus.commons.WorkflowStatusListener;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.DateRange;
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
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class L2ProductionTypeTest {

    private L2ProductionType productionType;

    @Before
    public void setUp() throws Exception {
        JobClient jobClient = new JobClient(new JobConf());
        HadoopProcessingService processingService = new HadoopProcessingService(jobClient) {
            @Override
            public BundleDescriptor[] getBundles(String filter) {
                ProcessorDescriptor processorDescriptor = new ProcessorDescriptor("BandMaths", "Band Arithmetic", "1.0",
                                                                                  "");
                processorDescriptor.setOutputProductType("Generic-L2");
                return new BundleDescriptor[]{new BundleDescriptor("beam", "4.9-SNAPSHOT", processorDescriptor)};
            }
        };
        productionType = new L2ProductionType(new TestInventoryService(),
                                              processingService,
                                              new TestStagingService());
    }

    @Test
    public void testGetDateRanges() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                                    "minDate", "2005-01-01",
                                                                    "maxDate", "2005-01-31");

        List<DateRange> dateRanges = L2ProductionType.getDateRanges(productionRequest);
        assertNotNull(dateRanges);
        assertEquals(1, dateRanges.size());
        DateRange dateRange = dateRanges.get(0);
        assertNotNull(dateRange);
        assertEquals(ProductionRequest.getDateFormat().parse("2005-01-01"), dateRange.getStartDate());
        assertEquals(ProductionRequest.getDateFormat().parse("2005-01-31"), dateRange.getStopDate());

        productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                  "minDate", "2005-01-01",
                                                  "maxDate", "2005-02-00");

        dateRanges = L2ProductionType.getDateRanges(productionRequest);
        assertNotNull(dateRanges);
        assertEquals(1, dateRanges.size());
        dateRange = dateRanges.get(0);
        assertNotNull(dateRange);
        assertEquals(ProductionRequest.getDateFormat().parse("2005-01-01"), dateRange.getStartDate());
        assertEquals(ProductionRequest.getDateFormat().parse("2005-01-31"), dateRange.getStopDate());

        productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                  "minDate", "2005-02-01",
                                                  "maxDate", "2005-03-00");

        dateRanges = L2ProductionType.getDateRanges(productionRequest);
        assertNotNull(dateRanges);
        assertEquals(1, dateRanges.size());
        dateRange = dateRanges.get(0);
        assertNotNull(dateRange);
        assertEquals(ProductionRequest.getDateFormat().parse("2005-02-01"), dateRange.getStartDate());
        assertEquals(ProductionRequest.getDateFormat().parse("2005-02-28"), dateRange.getStopDate());

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
        assertEquals("Level 2 BandMaths",
                     production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + L2ProductionType.NAME + "_"));
        WorkflowItem workflow = production.getWorkflow();
        assertNotNull(workflow);
        WorkflowItem[] workflowItems = workflow.getItems();
        assertNotNull(workflowItems);
        assertEquals(0, workflowItems.length);
        assertTrue(workflow instanceof L2WorkflowItem);

        L2WorkflowItem l2WorkflowItem = (L2WorkflowItem) workflow;
        assertEquals(true,
                     l2WorkflowItem.getOutputDir().startsWith("hdfs://master00:9000/calvalus/outputs/home/ewa/"));
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))",
                     l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        String inputFiles = l2WorkflowItem.getInputFiles();
        assertEquals("hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03", inputFiles);

        L2ProductionType.ProductSetSaver productSetSaver = getProductSetSaver(l2WorkflowItem);
        assertNotNull(productSetSaver);
        ProductSet productSet = productSetSaver.getProductSet();
        assertNotNull(productSet);
        assertEquals("Generic-L2", productSet.getProductType());
        assertEquals("Level 2 BandMaths",
                     productSet.getName());
        assertEquals(
                "hdfs://master00:9000/calvalus/outputs/home/ewa/" + production.getId() + "/.*${yyyy}${MM}${dd}.*.seq$",
                productSet.getPath());
        assertNull(productSet.getMinDate());
        assertNull(productSet.getMaxDate());
        assertNull(productSet.getRegionName());
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", productSet.getRegionWKT());
    }

    @Test
    public void testCalvalusJobNameIsUsedForProductionId() throws Exception {
        String nameForProduction = "my own name";
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "marcop",
                                                                    "inputPath", "MER_RR__1P/r03",
                                                                    "productionName",
                                                                    nameForProduction,
                                                                    "processorBundleName", "beam",
                                                                    "processorBundleVersion", "4.9-SNAPSHOT",
                                                                    "processorName", "BandMaths");

        Production production = productionType.createProduction(productionRequest);
        assertNotNull(production);
        assertEquals(nameForProduction, production.getName());
    }

    @Test
    public void testCreateProductionWithMinMaxDates() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                                    "inputPath", "MER_RR__1P/r03/${yyyy}/${MM}/${dd}",
                                                                    "productionName", "My Math Production",
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
        assertEquals("My Math Production", production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + L2ProductionType.NAME + "_"));
        WorkflowItem workflow = production.getWorkflow();
        assertNotNull(workflow);
        WorkflowItem[] workflowItems = workflow.getItems();
        assertNotNull(workflowItems);
        assertEquals(0, workflowItems.length);
        assertTrue(workflow instanceof L2WorkflowItem);
        L2WorkflowItem l2WorkflowItem = (L2WorkflowItem) workflow;
        assertEquals(true,
                     l2WorkflowItem.getOutputDir().startsWith("hdfs://master00:9000/calvalus/outputs/home/ewa/"));
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))",
                     l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        String inputFiles = l2WorkflowItem.getInputFiles();
        assertEquals("" +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/01," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/02," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/03," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/04," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/05," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/06," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/07," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/08," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/09," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/10," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/11," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/12," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/13," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/14," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/15," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/16," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/17," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/18," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/19," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/20," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/21," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/22," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/23," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/24," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/25," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/26," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/27," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/28," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/29," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/30," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/31",
                     inputFiles);

        L2ProductionType.ProductSetSaver productSetSaver = getProductSetSaver(l2WorkflowItem);
        assertNotNull(productSetSaver);
        ProductSet productSet = productSetSaver.getProductSet();
        assertNotNull(productSet);
        assertEquals("Generic-L2", productSet.getProductType());
        assertEquals("My Math Production", productSet.getName());
        assertEquals(
                "hdfs://master00:9000/calvalus/outputs/home/ewa/" + production.getId() + "/.*${yyyy}${MM}${dd}.*.seq$",
                productSet.getPath());
        assertEquals("2005-01-01", ProductionRequest.getDateFormat().format(productSet.getMinDate()));
        assertEquals("2005-01-31", ProductionRequest.getDateFormat().format(productSet.getMaxDate()));
        assertNull(productSet.getRegionName());
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", productSet.getRegionWKT());

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
                                                                    "regionName", "Island In The Sun",
                                                                    "regionWKT",
                                                                    "POLYGON ((5 55, 25 50, 25 60, 5 60, 5 55))"
        );

        Production production = productionType.createProduction(productionRequest);
        assertNotNull(production);
        assertEquals(
                "Level 2 BandMaths 2005-01-01 to 2005-01-31 (Island In The Sun)",
                production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + L2ProductionType.NAME + "_"));
        WorkflowItem workflow = production.getWorkflow();
        assertNotNull(workflow);
        WorkflowItem[] workflowItems = workflow.getItems();
        assertNotNull(workflowItems);
        assertEquals(0, workflowItems.length);
        assertTrue(workflow instanceof L2WorkflowItem);
        L2WorkflowItem l2WorkflowItem = (L2WorkflowItem) workflow;
        assertEquals(true,
                     l2WorkflowItem.getOutputDir().startsWith("hdfs://master00:9000/calvalus/outputs/home/ewa/"));
        assertEquals("POLYGON ((5 55, 25 50, 25 60, 5 60, 5 55))",
                     l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        String inputFiles = l2WorkflowItem.getInputFiles();
        assertEquals("" +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/01," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/15," +
                             "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2005/01/31",
                     inputFiles);

        L2ProductionType.ProductSetSaver productSetSaver = getProductSetSaver(l2WorkflowItem);
        assertNotNull(productSetSaver);
        ProductSet productSet = productSetSaver.getProductSet();
        assertNotNull(productSet);
        assertEquals("Generic-L2", productSet.getProductType());
        assertEquals(
                "Level 2 BandMaths 2005-01-01 to 2005-01-31 (Island In The Sun)",
                productSet.getName());
        assertEquals(
                "hdfs://master00:9000/calvalus/outputs/home/ewa/" + production.getId() + "/.*${yyyy}${MM}${dd}.*.seq$",
                productSet.getPath());
        assertEquals("2005-01-01", ProductionRequest.getDateFormat().format(productSet.getMinDate()));
        assertEquals("2005-01-31", ProductionRequest.getDateFormat().format(productSet.getMaxDate()));
        assertEquals("Island In The Sun", productSet.getRegionName());
        assertEquals("POLYGON ((5 55, 25 50, 25 60, 5 60, 5 55))", productSet.getRegionWKT());

    }

    private L2ProductionType.ProductSetSaver getProductSetSaver(L2WorkflowItem l2WorkflowItem) {
        WorkflowStatusListener[] workflowStatusListeners = l2WorkflowItem.getWorkflowStatusListeners();
        for (WorkflowStatusListener workflowStatusListener : workflowStatusListeners) {
            if (workflowStatusListener instanceof L2ProductionType.ProductSetSaver) {
                return (L2ProductionType.ProductSetSaver) workflowStatusListener;
            }
        }
        return null;
    }

}
