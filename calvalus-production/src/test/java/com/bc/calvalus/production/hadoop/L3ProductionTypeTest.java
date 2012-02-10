package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.production.*;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

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
        assertEquals("Level 3 BandMaths 2010-06-15 to 2010-08-15", production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + L3ProductionType.NAME + "_"));
        assertNotNull(production.getWorkflow());
        assertNotNull(production.getWorkflow().getItems());
        assertEquals(3, production.getWorkflow().getItems().length);

        // Note that periodLength=20 and compositingPeriodLength=5
        testItem(production.getWorkflow().getItems()[0], "2010-06-15", "2010-06-19");
        testItem(production.getWorkflow().getItems()[1], "2010-07-05", "2010-07-09");
        testItem(production.getWorkflow().getItems()[2], "2010-07-25", "2010-07-29");
    }

    private void testItem(WorkflowItem item, String date1, String date2) {

        assertTrue(item instanceof L3WorkflowItem);

        L3WorkflowItem l3WorkflowItem = (L3WorkflowItem) item;
        assertEquals(date1, l3WorkflowItem.getMinDate());
        assertEquals(date2, l3WorkflowItem.getMaxDate());
        assertEquals(true, l3WorkflowItem.getOutputDir().startsWith("hdfs://cvmaster00:9000/calvalus/outputs/home/ewa/"));
    }


    @Test
    public void testCreateL3Config() throws ProductionException {
        ProductionRequest productionRequest = createValidL3ProductionRequest();
        L3Config l3Config = L3ProductionType.getL3Config(productionRequest);
        assertNotNull(l3Config);
        assertEquals(4320, l3Config.getBinningContext().getBinningGrid().getNumRows());
        assertEquals("NOT INVALID", l3Config.getVariableContext().getMaskExpr());
        assertNotNull(l3Config.getSuperSampling());
        assertEquals(1, (int) l3Config.getSuperSampling());
        assertEquals(3, l3Config.getVariableContext().getVariableCount());
        assertEquals("a", l3Config.getVariableContext().getVariableName(0));
        assertEquals("b", l3Config.getVariableContext().getVariableName(1));
        assertEquals("c", l3Config.getVariableContext().getVariableName(2));
        BinManager binManager = l3Config.getBinningContext().getBinManager();
        assertEquals(3, binManager.getAggregatorCount());
        assertEquals("MIN_MAX", binManager.getAggregator(0).getName());
        assertEquals(2, binManager.getAggregator(0).getOutputFeatureNames().length);
        assertEquals(-999.9F, binManager.getAggregator(0).getOutputFillValue(), 1E-5F);
    }

    @Test
    public void testGeoRegion() throws ProductionException {
        ProductionRequest productionRequest = createValidL3ProductionRequest();
        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        assertNotNull(regionGeometry);
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", regionGeometry.toString());
    }

    /*
    @Test
    public void testCreateFormatterConfig() throws ProductionException {
        L3FormatterConfig formatterConfig = L3ProductionType.
        assertNotNull(formatterConfig);
        assertEquals("NetCDF", formatterConfig.getOutputFormat());
        assertEquals(new File("/opt/tomcat/webapps/calvalus/staging/ewa-A25F/L3_2010-06-03-2010-06-05.nc").getPath(), formatterConfig.getOutputFile());
        assertEquals("Product", formatterConfig.getOutputType());
    }
    */

    @Test
    public void testComputeBinningGridRowCount() {
        assertEquals(2160, L3ProductionType.computeBinningGridRowCount(9.28));
        assertEquals(2160 * 2, L3ProductionType.computeBinningGridRowCount(9.28 / 2));
        assertEquals(2160 / 2, L3ProductionType.computeBinningGridRowCount(9.28 * 2));
        assertEquals(66792, L3ProductionType.computeBinningGridRowCount(0.300)); //MERIS FR equivalent (300m)
        assertEquals(50094, L3ProductionType.computeBinningGridRowCount(0.400)); //400 m
    }

    @Test
    public void testGetDatePairList_MinMax() throws ProductionException, ParseException {
        ProductionRequest productionRequest = new ProductionRequest(L3ProductionType.NAME, "ewa",
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
        ProductionRequest productionRequest = new ProductionRequest(L3ProductionType.NAME, "ewa",
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
        return new ProductionRequest(L3ProductionType.NAME, "ewa",
                                     // GeneralLevel 3 parameters
                                     "inputPath", "MER_RR__1P/r03/2010",
                                     "outputFormat", "NetCDF",
                                     "autoStaging", "true",
                                     "processorBundleName", "beam",
                                     "processorBundleVersion", "4.9-SNAPSHOT",
                                     "processorName", "BandMaths",
                                     "processorParameters", "<!-- no params -->",
                                     // Special Level 3 parameters
                                     "variables.count", "3",

                                     "variables.0.name", "a",
                                     "variables.0.aggregator", "MIN_MAX",
                                     "variables.0.weightCoeff", "1.0",
                                     "variables.0.fillValue", "-999.9",

                                     "variables.1.name", "b",
                                     "variables.1.aggregator", "MIN_MAX",
                                     "variables.1.weightCoeff", "1.0",
                                     "variables.1.fillValue", "-999.9",

                                     "variables.2.name", "c",
                                     "variables.2.aggregator", "MIN_MAX",
                                     "variables.2.weightCoeff", "1.0",
                                     "variables.2.fillValue", "-999.9",

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
