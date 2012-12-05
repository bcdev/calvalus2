package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.processing.ta.TAConfig;
import com.bc.calvalus.processing.ta.TAWorkflowItem;
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

public class TAProductionTypeTest {

    private TAProductionType productionType;

    @Before
    public void setUp() throws Exception {
        JobClient jobClient = new JobClient(new JobConf());
        productionType = new TAProductionType(new TestInventoryService(),
                                              new HadoopProcessingService(jobClient),
                                              new TestStagingService());
    }

    @Test
    public void testCreateProduction() throws ProductionException, IOException {
        ProductionRequest productionRequest = createValidTAProductionRequest();
        Production production = productionType.createProduction(productionRequest);
        assertNotNull(production);
        assertEquals("Trend analysis BandMaths 2010-06-01 to 2010-06-15 (wonderland)", production.getName());
        assertEquals(true, production.getStagingPath().startsWith("ewa/"));
        assertEquals(true, production.getId().contains("_" + "TA" + "_"));
        assertNotNull(production.getWorkflow());
        assertNotNull(production.getWorkflow().getItems());
        assertEquals(5, production.getWorkflow().getItems().length);

        // Note that periodLength=compositingPeriodLength=3
        testItem(production.getWorkflow().getItems()[0], "2010-06-01", "2010-06-03");
        testItem(production.getWorkflow().getItems()[1], "2010-06-04", "2010-06-06");
        testItem(production.getWorkflow().getItems()[2], "2010-06-07", "2010-06-09");
        testItem(production.getWorkflow().getItems()[3], "2010-06-10", "2010-06-12");
        testItem(production.getWorkflow().getItems()[4], "2010-06-13", "2010-06-15");
    }

    private void testItem(WorkflowItem item1, String date1, String date2) {
        assertNotNull(item1.getItems());
        assertEquals(2, item1.getItems().length);

        assertTrue(item1.getItems()[0] instanceof L3WorkflowItem);
        L3WorkflowItem l3WorkflowItem = (L3WorkflowItem) item1.getItems()[0];
        assertEquals(date1, l3WorkflowItem.getMinDate());
        assertEquals(date2, l3WorkflowItem.getMaxDate());
        assertEquals(true, l3WorkflowItem.getOutputDir().startsWith("hdfs://master00:9000/calvalus/outputs/home/ewa/"));

        assertTrue(item1.getItems()[1] instanceof TAWorkflowItem);
        TAWorkflowItem taWorkflowItem = (TAWorkflowItem) item1.getItems()[1];
        assertEquals(date1, taWorkflowItem.getMinDate());
        assertEquals(date2, taWorkflowItem.getMaxDate());
        assertEquals(l3WorkflowItem.getOutputDir(), taWorkflowItem.getInputDir());
        assertFalse(l3WorkflowItem.getOutputDir().equals(taWorkflowItem.getOutputDir()));
        assertEquals(true, taWorkflowItem.getOutputDir().startsWith("hdfs://master00:9000/calvalus/outputs/home/ewa/"));
    }


    @Test
    public void testCreateTAConfig() throws ProductionException {
        ProductionRequest productionRequest = createValidTAProductionRequest();
        TAConfig taConfig = TAProductionType.getTAConfig(productionRequest);
        assertNotNull(taConfig);
        TAConfig.RegionConfiguration[] taConfigRegions = taConfig.getRegions();
        assertNotNull(taConfigRegions);
        assertEquals(1, taConfigRegions.length);
        TAConfig.RegionConfiguration taConfigRegion = taConfigRegions[0];
        assertNotNull(taConfigRegion);
        assertEquals("wonderland", taConfigRegion.getName());
        assertEquals("POLYGON ((-100 -50, 100 -50, 100 50, -100 50, -100 -50))", taConfigRegion.getGeometry().toString());
    }

    static ProductionRequest createValidTAProductionRequest() {
        return new ProductionRequest("TA", "ewa",
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
                                     "minDate", "2010-06-01",
                                     "maxDate", "2010-06-15",
                                     "periodLength", "3",
                                     "regionName", "wonderland",
                                     "regionWKT", "POLYGON((-100 -50, 100 -50, 100 50, -100 50, -100 -50))",
                                     "maxLon", "25",
                                     "minLat", "50",
                                     "maxLat", "60",
                                     "resolution", "4.64",
                                     "superSampling", "1"
        );
    }

}
