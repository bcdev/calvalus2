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

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.commons.WorkflowStatusListener;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.MaskDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestFileSystemService;
import com.bc.calvalus.production.TestStagingService;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class L2ProductionTypeTest {

    private L2ProductionType productionType;

    @Before
    public void setUp() throws Exception {
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf());
        HadoopProcessingService processingService = new HadoopProcessingService(jobClientsMap) {
            @Override
            public BundleDescriptor[] getBundles(String username, BundleFilter filter) {
                ProcessorDescriptor processorDescriptor = new ProcessorDescriptor("BandMaths", "Band Arithmetic", "1.0", "");
                processorDescriptor.setOutputProductType("Generic-L2");
                return new BundleDescriptor[]{new BundleDescriptor("beam", "4.9-SNAPSHOT", "/software/test/system", processorDescriptor)};
            }

            @Override
            public MaskDescriptor[] getMasks(String userName) {
                return new MaskDescriptor[0];
            }
        };
        productionType = new L2ProductionType(new TestFileSystemService(), processingService, new TestStagingService());
    }

    @Test
    public void testGetDateRanges() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest("L2", "ewa",
                                                                    "minDate", "2005-01-01",
                                                                    "maxDate", "2005-01-31");

        List<DateRange> dateRanges = productionRequest.getDateRanges();
        assertNotNull(dateRanges);
        assertEquals(1, dateRanges.size());
        DateRange dateRange = dateRanges.get(0);
        assertNotNull(dateRange);
        assertEquals(ProductionRequest.getDateFormat().parse("2005-01-01"), dateRange.getStartDate());
        assertEquals(ProductionRequest.getDateFormat().parse("2005-01-31"), dateRange.getStopDate());

        productionRequest = new ProductionRequest("L2", "ewa",
                                                  "minDate", "2005-01-01",
                                                  "maxDate", "2005-02-00");

        dateRanges = productionRequest.getDateRanges();
        assertNotNull(dateRanges);
        assertEquals(1, dateRanges.size());
        dateRange = dateRanges.get(0);
        assertNotNull(dateRange);
        assertEquals(ProductionRequest.getDateFormat().parse("2005-01-01"), dateRange.getStartDate());
        assertEquals(ProductionRequest.getDateFormat().parse("2005-01-31"), dateRange.getStopDate());

        productionRequest = new ProductionRequest("L2", "ewa",
                                                  "minDate", "2005-02-01",
                                                  "maxDate", "2005-03-00");

        dateRanges = productionRequest.getDateRanges();
        assertNotNull(dateRanges);
        assertEquals(1, dateRanges.size());
        dateRange = dateRanges.get(0);
        assertNotNull(dateRange);
        assertEquals(ProductionRequest.getDateFormat().parse("2005-02-01"), dateRange.getStartDate());
        assertEquals(ProductionRequest.getDateFormat().parse("2005-02-28"), dateRange.getStopDate());

    }

    @Test
    public void testCreateProductionWithoutDates() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest("L2", "ewa",
                                                                    "inputPath", "MER_RR__1P/r03",
                                                                    "outputFormat", "NetCDF",
                                                                    "autoStaging", "true",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, "beam",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, "4.9-SNAPSHOT",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, "/software/test/system",
                                                                    ProcessorProductionRequest.PROCESSOR_NAME, "BandMaths",
                                                                    ProcessorProductionRequest.PROCESSOR_PARAMETERS, "<parameters/>",
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
        assertEquals(true, production.getId().contains("_" + "L2" + "_"));
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
        assertEquals("MER_RR__1P/r03", l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS));
        assertEquals("[null:null]", l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES));
        assertEquals("", l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_REGION_NAME));

        ProductSetSaver productSetSaver = getProductSetSaver(l2WorkflowItem);
        assertNotNull(productSetSaver);
        ProductSet productSet = productSetSaver.getProductSet();
        assertNotNull(productSet);
        assertEquals("Generic-L2", productSet.getProductType());
        assertEquals("Level 2 BandMaths",
                     productSet.getName());
        assertEquals(
                "hdfs://master00:9000/calvalus/outputs/home/ewa/" + production.getId() + "/[^_\\.].*(?:${yyyy}${MM}${dd}|${yyyy}${DDD}).*\\.seq$",
                productSet.getPath());
        assertNull(productSet.getMinDate());
        assertNull(productSet.getMaxDate());
        assertEquals("", productSet.getRegionName());
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", productSet.getRegionWKT());
    }

    @Test
    public void testCalvalusJobNameIsUsedForProductionId() throws Exception {
        String nameForProduction = "my own name";
        ProductionRequest productionRequest = new ProductionRequest("L2", "marcop",
                                                                    "inputPath", "MER_RR__1P/r03",
                                                                    "productionName",
                                                                    nameForProduction,
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, "beam",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, "4.9-SNAPSHOT",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, "/calvalus/test/bundles",
                                                                    ProcessorProductionRequest.PROCESSOR_NAME, "BandMaths");

        Production production = productionType.createProduction(productionRequest);
        assertNotNull(production);
        assertEquals(nameForProduction, production.getName());
    }

    @Test
    public void testCreateProductionWithMinMaxDates() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest("L2", "ewa",
                                                                    "inputPath", "MER_RR__1P/r03/${yyyy}/${MM}/${dd}",
                                                                    "productionName", "My Math Production",
                                                                    "minDate", "2005-01-01",
                                                                    "maxDate", "2005-01-31",
                                                                    "outputFormat", "NetCDF",
                                                                    "autoStaging", "true",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, "beam",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, "4.9-SNAPSHOT",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, "/software/test/system",
                                                                    ProcessorProductionRequest.PROCESSOR_NAME, "BandMaths",
                                                                    ProcessorProductionRequest.PROCESSOR_PARAMETERS, "<!-- no params -->",
                                                                    "minLon", "5",
                                                                    "maxLon", "25",
                                                                    "minLat", "50",
                                                                    "maxLat", "60"
        );

        Production production = productionType.createProduction(productionRequest);
        assertNotNull(production);
        assertEquals("My Math Production", production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + "L2" + "_"));
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
        String inputPathPattern = l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
        assertEquals("MER_RR__1P/r03/${yyyy}/${MM}/${dd}", inputPathPattern);
        assertEquals("[2005-01-01:2005-01-31]",
                     l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES));
        assertEquals("", l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_REGION_NAME));

        ProductSetSaver productSetSaver = getProductSetSaver(l2WorkflowItem);
        assertNotNull(productSetSaver);
        ProductSet productSet = productSetSaver.getProductSet();
        assertNotNull(productSet);
        assertEquals("Generic-L2", productSet.getProductType());
        assertEquals("My Math Production", productSet.getName());
        assertEquals(
                "hdfs://master00:9000/calvalus/outputs/home/ewa/" + production.getId() + "/[^_\\.].*(?:${yyyy}${MM}${dd}|${yyyy}${DDD}).*\\.seq$",
                productSet.getPath());
        assertEquals("2005-01-01", ProductionRequest.getDateFormat().format(productSet.getMinDate()));
        assertEquals("2005-01-31", ProductionRequest.getDateFormat().format(productSet.getMaxDate()));
        assertEquals("", productSet.getRegionName());
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", productSet.getRegionWKT());

    }

    @Test
    public void testCreateProductionWithDatelist() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest("L2", "ewa",
                                                                    "inputPath", "MER_RR__1P/r03/${yyyy}/${MM}/${dd}",
                                                                    "dateList", "2005-01-01 2005-01-15 2005-01-31",
                                                                    "outputFormat", "NetCDF",
                                                                    "autoStaging", "true",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, "beam",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, "4.9-SNAPSHOT",
                                                                    ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, "/software/test/system",
                                                                    ProcessorProductionRequest.PROCESSOR_NAME, "BandMaths",
                                                                    ProcessorProductionRequest.PROCESSOR_PARAMETERS, "<!-- no params -->",
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
        assertEquals(true, production.getId().contains("_" + "L2" + "_"));
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
        String inputPathPattern = l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);
        assertEquals("MER_RR__1P/r03/${yyyy}/${MM}/${dd}", inputPathPattern);
        assertEquals("[2005-01-01:2005-01-01],[2005-01-15:2005-01-15],[2005-01-31:2005-01-31]",
                     l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES));
        assertEquals("Island In The Sun", l2WorkflowItem.getJobConfig().get(JobConfigNames.CALVALUS_INPUT_REGION_NAME));

        ProductSetSaver productSetSaver = getProductSetSaver(l2WorkflowItem);
        assertNotNull(productSetSaver);
        ProductSet productSet = productSetSaver.getProductSet();
        assertNotNull(productSet);
        assertEquals("Generic-L2", productSet.getProductType());
        assertEquals(
                "Level 2 BandMaths 2005-01-01 to 2005-01-31 (Island In The Sun)",
                productSet.getName());
        assertEquals(
                "hdfs://master00:9000/calvalus/outputs/home/ewa/" + production.getId() + "/[^_\\.].*(?:${yyyy}${MM}${dd}|${yyyy}${DDD}).*\\.seq$",
                productSet.getPath());
        assertEquals("2005-01-01", ProductionRequest.getDateFormat().format(productSet.getMinDate()));
        assertEquals("2005-01-31", ProductionRequest.getDateFormat().format(productSet.getMaxDate()));
        assertEquals("Island In The Sun", productSet.getRegionName());
        assertEquals("POLYGON ((5 55, 25 50, 25 60, 5 60, 5 55))", productSet.getRegionWKT());

    }

    private ProductSetSaver getProductSetSaver(L2WorkflowItem l2WorkflowItem) {
        WorkflowStatusListener[] workflowStatusListeners = l2WorkflowItem.getWorkflowStatusListeners();
        for (WorkflowStatusListener workflowStatusListener : workflowStatusListeners) {
            if (workflowStatusListener instanceof ProductSetSaver) {
                return (ProductSetSaver) workflowStatusListener;
            }
        }
        return null;
    }

    @Test
    public void testPathPatternRegex() throws Exception {
        String[] files = {
                "MER_20050401X.N1",
                "L2_of_MER_20050401X.seq",
                "A2005111X.hdf",
                "L2_of_A2005111X.seq",
                ".MER_20050401X.N1",
                ".L2_of_MER_20050401X.seq",
                ".A2005111X.hdf",
                ".L2_of_A2005111X.seq",
                "_SUCCESS",
                "product-sets.csv"
        };
        // .*${yyyy}${MM}${dd}.*.seq$
        String pattern = "[^_\\.].*20050401.*\\.seq$";
        List<String> matches = match(files, pattern);
        assertEquals(1, matches.size());
        assertEquals(files[1], matches.get(0));

        // .*${yyyy}${DDD}.*.seq$
        pattern = "[^_\\.].*2005111.*\\.seq$";
        matches = match(files, pattern);
        assertEquals(1, matches.size());
        assertEquals(files[3], matches.get(0));

        // .*${yyyy}${DDD}.*.seq$ AND .*${yyyy}${MM}${dd}.*.seq$
        pattern = "[^_\\.].*2005111.*\\.seq$|[^_\\.].*20050401.*\\.seq$";
        matches = match(files, pattern);
        assertEquals(2, matches.size());
        assertEquals(files[1], matches.get(0));
        assertEquals(files[3], matches.get(1));


        // .*${yyyy}${MM}${dd}.*
        pattern = "[^_\\.].*20050401.*";
        matches = match(files, pattern);
        assertEquals(2, matches.size());
        assertEquals(files[0], matches.get(0));
        assertEquals(files[1], matches.get(1));

        // .*${yyyy}${DDD}.*
        pattern = "[^_\\.].*2005111.*";
        matches = match(files, pattern);
        assertEquals(2, matches.size());
        assertEquals(files[2], matches.get(0));
        assertEquals(files[3], matches.get(1));

        // .*${yyyy}${DDD}.* AND .*${yyyy}${MM}${dd}.*
        pattern = "[^_\\.].*2005111.*|[^_\\.].*20050401.*";
        matches = match(files, pattern);
        assertEquals(4, matches.size());
        assertEquals(files[0], matches.get(0));
        assertEquals(files[1], matches.get(1));
        assertEquals(files[2], matches.get(2));
        assertEquals(files[3], matches.get(3));
    }

    private List<String> match(String[] files, String pattern) {
        List<String> resultList = new ArrayList<>();
        for (String file : files) {
            if (file.matches(pattern)) {
                resultList.add(file);
            }
        }
        return resultList;
    }
}
