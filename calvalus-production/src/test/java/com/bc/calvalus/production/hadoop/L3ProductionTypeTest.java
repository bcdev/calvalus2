package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestFileSystemService;
import com.bc.calvalus.production.TestStagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.mapred.JobConf;
import org.esa.snap.binning.operator.BinningConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class L3ProductionTypeTest {

    private L3ProductionType productionType;

    @Before
    public void setUp() throws Exception {
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf());
        productionType = new L3ProductionType(new TestFileSystemService(),
                                              new HadoopProcessingService(jobClientsMap),
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
        BinningConfig binningConfig = L3ProductionType.getBinningConfig(productionRequest);
        assertNotNull(binningConfig);
        assertEquals(4320, binningConfig.getNumRows());
        assertEquals("NOT INVALID", binningConfig.getMaskExpr());
        assertNotNull(binningConfig.getSuperSampling());
        assertEquals(1, (int) binningConfig.getSuperSampling());
        assertEquals(0, binningConfig.getVariableConfigs().length);
        assertEquals(3, binningConfig.getAggregatorConfigs().length);
        assertEquals("MIN_MAX", binningConfig.getAggregatorConfigs()[0].getName());

        String xmlExpected = "<parameters>\n" +
                "  <planetaryGrid>org.esa.snap.binning.support.SEAGrid</planetaryGrid>\n" +
                "  <numRows>4320</numRows>\n" +
                "  <compositingType>BINNING</compositingType>\n" +
                "  <superSampling>1</superSampling>\n" +
                "  <maskExpr>NOT INVALID</maskExpr>\n" +
                "  <variables/>\n" +
                "  <aggregators>\n" +
                "    <aggregator>\n" +
                "      <type>MIN_MAX</type>\n" +
                "      <varName>a</varName>\n" +
                "    </aggregator>\n" +
                "    <aggregator>\n" +
                "      <type>MIN_MAX</type>\n" +
                "      <varName>b</varName>\n" +
                "    </aggregator>\n" +
                "    <aggregator>\n" +
                "      <type>MIN_MAX</type>\n" +
                "      <varName>c</varName>\n" +
                "    </aggregator>\n" +
                "  </aggregators>\n" +
                "</parameters>";
        assertEquals(xmlExpected, binningConfig.toXml());
    }

    @Test
    public void testGeoRegion() throws ProductionException {
        ProductionRequest productionRequest = createValidL3ProductionRequest();
        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        assertNotNull(regionGeometry);
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", regionGeometry.toString());
    }

    @Test
    public void testGetDatePairList_MinMax_complex() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest("L3", "ewa",
                                                                    "minDate", "2010-06-15",
                                                                    "maxDate", "2010-08-15",
                                                                    "periodLength", "20",
                                                                    "compositingPeriodLength", "5");

        List<DateRange> dateRangeList = L3ProductionType.getDateRanges(productionRequest, null);
        assertEquals(3, dateRangeList.size());
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-06-19", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-07-05", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-07-09", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-07-25", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-07-29", asString(dateRangeList.get(2).getStopDate()));
    }

    @Test
    public void testGetDatePairList_MinMax_simple() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest("L3", "ewa",
                                                                    "minDate", "2010-06-15",
                                                                    "maxDate", "2010-08-15",
                                                                    "periodLength", "20",
                                                                    "compositingPeriodLength", "20");

        List<DateRange> dateRangeList = L3ProductionType.getDateRanges(productionRequest, null);
        assertEquals(3, dateRangeList.size());
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-07-04", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-07-05", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-07-24", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-07-25", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-08-13", asString(dateRangeList.get(2).getStopDate()));
    }

    @Test
    public void testGetDatePairList_DateList() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest("L3", "ewa",
                                                                    "dateList", "2010-06-15 2010-07-01 2010-07-19 ");

        List<DateRange> dateRangeList = L3ProductionType.getDateRanges(productionRequest, null);
        assertEquals(3, dateRangeList.size());
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-07-01", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-07-01", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-07-19", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-07-19", asString(dateRangeList.get(2).getStopDate()));

    }

    @Test
    public void testGetDatePairList_Monthly() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest("L3", "ewa",
                                                                    "minDate", "2010-06-15",
                                                                    "maxDate", "2010-08-15",
                                                                    "periodLength", "-30");

        List<DateRange> dateRangeList = L3ProductionType.getDateRanges(productionRequest, null);
        assertEquals(2, dateRangeList.size());
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-07-14", asString(dateRangeList.get(0).getStopDate()));
        assertEquals("2010-07-15", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-08-14", asString(dateRangeList.get(1).getStopDate()));

        productionRequest = new ProductionRequest("L3", "ewa",
                                                  "minDate", "2010-06-15",
                                                  "maxDate", "2010-08-15",
                                                  "periodLength", "-30",
                                                  "compositingPeriodLength", "5");

        dateRangeList = L3ProductionType.getDateRanges(productionRequest, null);
        assertEquals(2, dateRangeList.size());
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-06-19", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-07-15", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-07-19", asString(dateRangeList.get(1).getStopDate()));

        // is this the expected result ???
        productionRequest = new ProductionRequest("L3", "ewa",
                                                  "minDate", "2010-01-01",
                                                  "maxDate", "2010-12-31",
                                                  "periodLength", "100",
                                                  "compositingPeriodLength", "-30");

        dateRangeList = L3ProductionType.getDateRanges(productionRequest, null);
        assertEquals(4, dateRangeList.size());
        assertEquals("2010-01-01", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-01-31", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-04-11", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-05-10", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-07-20", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-08-19", asString(dateRangeList.get(2).getStopDate()));

        assertEquals("2010-10-28", asString(dateRangeList.get(3).getStartDate()));
        assertEquals("2010-11-27", asString(dateRangeList.get(3).getStopDate()));
    }

    @Test
    public void testGetDatePairList_OverlappingPeriods() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest("L3", "ewa",
                                                                    "minDate", "2010-06-15",
                                                                    "maxDate", "2010-06-22",
                                                                    "periodLength", "1",
                                                                    "compositingPeriodLength", "5");

        List<DateRange> dateRangeList = L3ProductionType.getDateRanges(productionRequest, null);
        assertEquals(4, dateRangeList.size());
        assertEquals("2010-06-15", asString(dateRangeList.get(0).getStartDate()));
        assertEquals("2010-06-19", asString(dateRangeList.get(0).getStopDate()));

        assertEquals("2010-06-16", asString(dateRangeList.get(1).getStartDate()));
        assertEquals("2010-06-20", asString(dateRangeList.get(1).getStopDate()));

        assertEquals("2010-06-17", asString(dateRangeList.get(2).getStartDate()));
        assertEquals("2010-06-21", asString(dateRangeList.get(2).getStopDate()));

        assertEquals("2010-06-18", asString(dateRangeList.get(3).getStartDate()));
        assertEquals("2010-06-22", asString(dateRangeList.get(3).getStopDate()));
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

                                     "variables.0.aggregator", "MIN_MAX",
                                     "variables.0.weightCoeff", "1.0",
                                     "variables.0.parameter.count", "1",
                                     "variables.0.parameter.0.name", "varName",
                                     "variables.0.parameter.0.value", "a",
                                     "variables.0.parameter.1.name", "weightCoeff",
                                     "variables.0.parameter.1.value", "1.0",

                                     "variables.1.aggregator", "MIN_MAX",
                                     "variables.1.weightCoeff", "1.0",
                                     "variables.1.parameter.count", "1",
                                     "variables.1.parameter.0.name", "varName",
                                     "variables.1.parameter.0.value", "b",
                                     "variables.1.parameter.1.name", "weightCoeff",
                                     "variables.1.parameter.1.value", "1.0",

                                     "variables.2.aggregator", "MIN_MAX",
                                     "variables.2.weightCoeff", "1.0",
                                     "variables.2.parameter.count", "1",
                                     "variables.2.parameter.0.name", "varName",
                                     "variables.2.parameter.0.value", "c",
                                     "variables.2.parameter.1.name", "weightCoeff",
                                     "variables.2.parameter.1.value", "1.0",

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
