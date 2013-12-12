package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestInventoryService;
import com.bc.calvalus.production.TestStagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.esa.beam.binning.BinManager;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class L3ProductionTypeTest {

    private L3ProductionType productionType;

    @Before
    public void setUp() throws Exception {
        JobClient jobClient = new JobClient(new JobConf());
        productionType = new L3ProductionType(new TestInventoryService(),
                                              new HadoopProcessingService(jobClient),
                                              new TestStagingService());
    }

    @Test
    public void testCreateProduction() throws ProductionException, IOException {
        ProductionRequest productionRequest = createValidL3ProductionRequest();
        Production production = productionType.createProduction(productionRequest);

        assertNotNull(production);
        assertEquals("Level-3 BandMaths 2010-06-15 to 2010-08-15", production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + "L3" + "_"));
        WorkflowItem workflow = production.getWorkflow();
        assertNotNull(workflow);
        assertSame(Workflow.Sequential.class, workflow.getClass());
        WorkflowItem[] sequenceWfs = workflow.getItems();
        assertNotNull(sequenceWfs);
        assertEquals(2, sequenceWfs.length);

        WorkflowItem wfL3Parallel = sequenceWfs[0];
        assertSame(Workflow.Parallel.class, wfL3Parallel.getClass());
        WorkflowItem[] parallelWfs = wfL3Parallel.getItems();
        assertNotNull(parallelWfs);
        assertEquals(3, parallelWfs.length);
        // Note that periodLength=20 and compositingPeriodLength=5
        testL3Item(parallelWfs[0], "2010-06-15", "2010-06-19");
        testL3Item(parallelWfs[1], "2010-07-05", "2010-07-09");
        testL3Item(parallelWfs[2], "2010-07-25", "2010-07-29");

        WorkflowItem wfFormat = sequenceWfs[1];
        assertSame(L3FormatWorkflowItem.class, wfFormat.getClass());
        L3FormatWorkflowItem l3formatWorkflowItem = (L3FormatWorkflowItem) wfFormat;
        assertEquals(true,
                     l3formatWorkflowItem.getInputDir().startsWith("hdfs://master00:9000/calvalus/outputs/home/ewa/"));
        assertEquals(true,
                     l3formatWorkflowItem.getOutputDir().startsWith("hdfs://master00:9000/calvalus/outputs/home/ewa/"));
    }

    private void testL3Item(WorkflowItem item, String date1, String date2) {
        assertSame(L3WorkflowItem.class, item.getClass());
        L3WorkflowItem l3WorkflowItem = (L3WorkflowItem) item;

        assertEquals(date1, l3WorkflowItem.getMinDate());
        assertEquals(date2, l3WorkflowItem.getMaxDate());
        assertEquals(true, l3WorkflowItem.getOutputDir().startsWith("hdfs://master00:9000/calvalus/outputs/home/ewa/"));
    }


    @Test
    public void testCreateL3Config() throws ProductionException {
        ProductionRequest productionRequest = createValidL3ProductionRequest();
        L3Config l3Config = L3ProductionType.getL3Config(productionRequest);
        assertNotNull(l3Config);
        assertEquals(4320, l3Config.createBinningContext(null).getPlanetaryGrid().getNumRows());
        assertEquals("NOT INVALID", l3Config.createVariableContext().getValidMaskExpression());
        assertNotNull(l3Config.getSuperSampling());
        assertEquals(1, (int) l3Config.getSuperSampling());
        assertEquals(3, l3Config.createVariableContext().getVariableCount());
        assertEquals("a", l3Config.createVariableContext().getVariableName(0));
        assertEquals("b", l3Config.createVariableContext().getVariableName(1));
        assertEquals("c", l3Config.createVariableContext().getVariableName(2));
        BinManager binManager = l3Config.createBinningContext(null).getBinManager();
        assertEquals(3, binManager.getAggregatorCount());
        assertEquals("MIN_MAX", binManager.getAggregator(0).getName());
        assertEquals(2, binManager.getAggregator(0).getOutputFeatureNames().length);
    }

    @Test
    public void testGeoRegion() throws ProductionException {
        ProductionRequest productionRequest = createValidL3ProductionRequest();
        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        assertNotNull(regionGeometry);
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", regionGeometry.toString());
    }

    @Test
    public void testGetDatePairList_MinMax() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest("L3", "ewa",
                                                                    "minDate", "2010-06-15",
                                                                    "maxDate", "2010-08-15",
                                                                    "periodLength", "20",
                                                                    "compositingPeriodLength", "5");

        List<DateRange> dateRangeList = L3ProductionType.getDateRanges(productionRequest, 10);
        assertEquals(3, dateRangeList.size());
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-06-19", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-07-05", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-07-09", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-07-25", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-07-29", asString(dateRangeList.get(2).getStopDate()));
    }

    @Test
    public void testGetDatePairList_DateList() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest("L3", "ewa",
                                                                    "dateList", "2010-06-15 2010-07-01 2010-07-19 ");

        List<DateRange> dateRangeList = L3ProductionType.getDateRanges(productionRequest, 10);
        assertEquals(3, dateRangeList.size());
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-07-01", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-07-01", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-07-19", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-07-19", asString(dateRangeList.get(2).getStopDate()));

    }

    private static String asString(Date date) {
        return ProductionRequest.getDateFormat().format(date);
    }

    static ProductionRequest createValidL3ProductionRequest() {
        return new ProductionRequest("L3", "ewa",
                                     // GeneralLevel 3 parameters
                                     "inputPath", "MER_RR__1P/r03/2010",
                                     "outputFormat", "NetCDF",
                                     "autoStaging", "true",
                                     ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, "beam",
                                     ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, "4.9-SNAPSHOT",
                                     ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, "/calvalus/test/bundles",
                                     ProcessorProductionRequest.PROCESSOR_NAME, "BandMaths",
                                     ProcessorProductionRequest.PROCESSOR_PARAMETERS, "<!-- no params -->",
                                     // Special Level 3 parameters
                                     "variables.count", "3",

                                     "variables.0.name", "a",
                                     "variables.0.aggregator", "MIN_MAX",
                                     "variables.0.weightCoeff", "1.0",

                                     "variables.1.name", "b",
                                     "variables.1.aggregator", "MIN_MAX",
                                     "variables.1.weightCoeff", "1.0",

                                     "variables.2.name", "c",
                                     "variables.2.aggregator", "MIN_MAX",
                                     "variables.2.weightCoeff", "1.0",

                                     "maskExpr", "NOT INVALID",
                                     "minDate", "2010-06-15",
                                     "maxDate", "2010-08-15",
                                     "periodLength", "20",
                                     "compositingPeriodLength", "5",
                                     "minLon", "5",
                                     "maxLon", "25",
                                     "minLat", "50",
                                     "maxLat", "60",
                                     "resolution", "4.64",
                                     "superSampling", "1"
        );
    }

}
