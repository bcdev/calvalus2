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
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestStagingService;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class L2ProductionTypeTest {

    private List<CallArguments> callArgumentsList;

    private static class CallArguments {
        String inputProductSetId;
        Date minDate;
        Date maxDate;

        private CallArguments(String inputProductSetId, Date minDate, Date maxDate) {
            this.inputProductSetId = inputProductSetId;
            this.minDate = minDate;
            this.maxDate = maxDate;
        }
    }

    private L2ProductionType productionType;

    @Before
    public void setUp() throws Exception {
        callArgumentsList = new ArrayList<CallArguments>();
        productionType = new L2ProductionType(new HadoopProcessingService(new JobClient(new JobConf())), new TestStagingService()) {
            @Override
            public String[] getInputFiles(String inputProductSetId, Date minDate, Date maxDate) throws ProductionException {
                callArgumentsList.add(new CallArguments(inputProductSetId, minDate, maxDate));
                return new String[]{"MER_RR_007.N1"};
            }
        };
    }

    @Test
    public void testCreateProductionWithoutDates() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
//                                                                  "inputProductSetId", "MER_RR__1P/r03/2010",
                                                                    "inputProductSetId", "MER_RR__1P/r03",
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
        assertEquals("Level 2 production using product set 'MER_RR__1P/r03' and L2 processor 'BandMaths'", production.getName());
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

        assertEquals(1, callArgumentsList.size());
        assertCallArguments(callArgumentsList.get(0), "MER_RR__1P/r03", null, null);
    }

    @Test
    public void testCreateProductionWithMinMaxDates() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                                    "inputProductSetId", "MER_RR__1P/r03",
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
        assertEquals("Level 2 production using product set 'MER_RR__1P/r03' and L2 processor 'BandMaths'", production.getName());
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

        assertEquals(1, callArgumentsList.size());
        assertCallArguments(callArgumentsList.get(0), "MER_RR__1P/r03", "2005-01-01", "2005-01-31");
    }

    @Test
    public void testCreateProductionWithDatelist() throws ProductionException, IOException {
        ProductionRequest productionRequest = new ProductionRequest(L2ProductionType.NAME, "ewa",
                                                                    "inputProductSetId", "MER_RR__1P/r03",
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
        assertEquals("Level 2 production using product set 'MER_RR__1P/r03' and L2 processor 'BandMaths'", production.getName());
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

        assertEquals(3, callArgumentsList.size());
        assertCallArguments(callArgumentsList.get(0), "MER_RR__1P/r03", "2005-01-01", "2005-01-01");
        assertCallArguments(callArgumentsList.get(1), "MER_RR__1P/r03", "2005-01-15", "2005-01-15");
        assertCallArguments(callArgumentsList.get(2), "MER_RR__1P/r03", "2005-01-31", "2005-01-31");
    }

    private void assertCallArguments(CallArguments callArguments, String productSetId, String minDate, String maxDate) {
        assertNotNull(callArguments);
        assertEquals(productSetId, callArguments.inputProductSetId);
        assertDate("minDate", minDate, callArguments.minDate);
        assertDate("maxDate", maxDate, callArguments.maxDate);
    }

    private void assertDate(String message, String expectedDate, Date date) {
        if (expectedDate == null) {
            assertNull(message, date);
        } else {
            String actual = ProductionRequest.getDateFormat().format(date);
            assertEquals(message, expectedDate, actual);
        }
    }
}
