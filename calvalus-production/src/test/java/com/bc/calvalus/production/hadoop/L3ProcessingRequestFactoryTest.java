package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.processing.beam.L3Config;
import com.bc.calvalus.processing.beam.L3FormatterConfig;
import com.bc.calvalus.processing.hadoop.L3ProcessingRequest;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestProcessingService;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.*;

public class L3ProcessingRequestFactoryTest {
    @Test
    public void testGetProcessingParameters() throws ProductionException {

        ProductionRequest productionRequest = createValidL3ProductionRequest();
        L3ProcessingRequestFactory requestFactory = new L3ProcessingRequestFactory(new TestProcessingService()
        );
        L3ProcessingRequest[] processingRequests = requestFactory.createWorkflowItems("A25F", productionRequest);
        assertNotNull(processingRequests);
        assertEquals(1, processingRequests.length);

        L3ProcessingRequest processingRequest = processingRequests[0];

        // Assert that derived processing parameters are generated correctly
        assertEquals(3 * 4, processingRequest.getInputFiles().length);
        assertEquals("hdfs://cvmaster00:9000/calvalus/output/ewa/A25F_0", processingRequest.getOutputDir());
        assertEquals(true, processingRequest.isAutoStaging());

        // Assert that derived processing parameters are present in map
        Map<String, Object> processingParameters = processingRequest.getProcessingParameters();
        assertNotNull(processingParameters);
        assertNotNull(processingParameters.get("inputFiles"));
        assertEquals("hdfs://cvmaster00:9000/calvalus/output/ewa/A25F_0", processingParameters.get("outputDir"));
        assertEquals("MER_RR__1P/r03/2010", processingParameters.get("inputProductSetId"));
        assertEquals("beam", processingParameters.get("processorBundleName"));
        assertEquals("4.9-SNAPSHOT", processingParameters.get("processorBundleVersion"));
        assertEquals("BandMaths", processingParameters.get("processorName"));
        assertEquals("<!-- no params -->", processingParameters.get("processorParameters"));

        // Assert that processing config objects are correct
        L3Config l3Config = processingRequest.getBeamL3Config();
        assertNotNull(l3Config);
        assertEquals(4320, l3Config.getBinningContext().getBinningGrid().getNumRows());
        assertEquals("NOT INVALID", l3Config.getVariableContext().getMaskExpr());
        float[] superSamplingSteps = l3Config.getSuperSamplingSteps();
        assertEquals(1, superSamplingSteps.length);
        assertEquals(0.5f, superSamplingSteps[0], 1e-5);
        Geometry regionOfInterest = l3Config.getRegionOfInterest();
        assertNotNull(regionOfInterest);
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", regionOfInterest.toString());
        assertEquals(3, l3Config.getVariableContext().getVariableCount());
        assertEquals("a", l3Config.getVariableContext().getVariableName(0));
        assertEquals(" b", l3Config.getVariableContext().getVariableName(1));
        assertEquals(" c", l3Config.getVariableContext().getVariableName(2));
        BinManager binManager = l3Config.getBinningContext().getBinManager();
        assertEquals(3, binManager.getAggregatorCount());
        assertEquals("MIN_MAX", binManager.getAggregator(0).getName());
        assertEquals(2, binManager.getAggregator(0).getOutputPropertyCount());
        assertEquals(-999.9, binManager.getAggregator(0).getOutputPropertyFillValue(0), 1E-5);
        assertEquals(-999.9, binManager.getAggregator(0).getOutputPropertyFillValue(1), 1E-5);

        L3FormatterConfig formatterConfig = processingRequest.getFormatterL3Config("/opt/tomcat/webapps/calvalus/staging/ewa-A25F");
        assertNotNull(formatterConfig);
        assertEquals("NetCDF", formatterConfig.getOutputFormat());
        assertEquals(new File("/opt/tomcat/webapps/calvalus/staging/ewa-A25F/L3_2010-06-03-2010-06-05.nc").getPath(), formatterConfig.getOutputFile());
        assertEquals("Product", formatterConfig.getOutputType());
    }

    static ProductionRequest createValidL3ProductionRequest() {
        return new ProductionRequest("calvalus-level3", "ewa",
                                     // GeneralLevel 3 parameters
                                     "inputProductSetId", "MER_RR__1P/r03/2010",
                                     "outputFormat", "NetCDF",
                                     "autoStaging", "true",
                                     "processorBundleName", "beam",
                                     "processorBundleVersion", "4.9-SNAPSHOT",
                                     "processorName", "BandMaths",
                                     "processorParameters", "<!-- no params -->",
                                     // Special Level 3 parameters
                                     "inputVariables", "a, b, c",
                                     "maskExpr", "NOT INVALID",
                                     "aggregator", "MIN_MAX",
                                     "weightCoeff", "1.0",
                                     "dateStart", "2010-06-03",
                                     "dateStop", "2010-06-05",
                                     "periodCount", "1",
                                     "periodLength", "3",
                                     "lonMin", "5",
                                     "lonMax", "25",
                                     "latMin", "50",
                                     "latMax", "60",
                                     "resolution", "4.64",
                                     "fillValue", "-999.9",
                                     "superSampling", "1"
        );
    }


    @Test
    public void testComputeNumRows() {
        assertEquals(2160, L3ProcessingRequestFactory.computeBinningGridRowCount(9.28));
        assertEquals(2160 * 2, L3ProcessingRequestFactory.computeBinningGridRowCount(9.28 / 2));
        assertEquals(2160 / 2, L3ProcessingRequestFactory.computeBinningGridRowCount(9.28 * 2));
    }
}
