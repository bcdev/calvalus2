package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.processing.beam.FormatterL3Config;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestProcessingService;
import com.bc.calvalus.production.TestStagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class L3ProcessingRequestTest {
    @Test
    public void testGetProcessingParameters() throws ProductionException {

        ProductionRequest productionRequest = createValidL3ProductionRequest();
        L3ProcessingRequestFactory requestFactory = new L3ProcessingRequestFactory(new TestProcessingService(),
                                                                                   new TestStagingService());
        L3ProcessingRequest[] processingRequests = requestFactory.createProcessingRequests("A25F", "ewa", productionRequest);
        assertNotNull(processingRequests);
        assertEquals(1, processingRequests.length);

         L3ProcessingRequest processingRequest = processingRequests[0];

        // Assert that derived processing parameters are generated correctly
        assertEquals(3 * 4, processingRequest.getInputFiles().length);
        assertEquals(3, processingRequest.getAggregators().length);
        assertEquals(0, processingRequest.getVariables().length);
        assertEquals("5,50,25,60", processingRequest.getBBox());
        assertEquals(4320, (int) processingRequest.getNumRows());
        assertEquals("hdfs://cvmaster00:9000/calvalus/output/ewa/A25F_0", processingRequest.getOutputDir());
        assertEquals("/opt/tomcat/webapps/calvalus/staging/ewa-A25F/out", processingRequest.getStagingDir());
        assertEquals(true, processingRequest.isAutoStaging());
        assertEquals(true, Double.isNaN(processingRequest.getFillValue()));

        // Assert that derived processing parameters are present in map
        Map<String, Object> processingParameters = processingRequest.getProcessingParameters();
        assertNotNull(processingParameters);
        assertNotNull(processingParameters.get("inputFiles"));
        assertNotNull(processingParameters.get("variables"));
        assertNotNull(processingParameters.get("aggregators"));
        assertEquals("5,50,25,60", processingParameters.get("bbox"));
        assertEquals(4320, processingParameters.get("numRows"));
        assertEquals("hdfs://cvmaster00:9000/calvalus/output/ewa/A25F_0", processingParameters.get("outputDir"));
        assertEquals("/opt/tomcat/webapps/calvalus/staging/ewa-A25F/out", processingParameters.get("stagingDir"));
        assertEquals("MER_RR__1P/r03/2010", processingParameters.get("inputProductSetId"));
        assertEquals("beam", processingParameters.get("l2ProcessorBundleName"));
        assertEquals("4.9-SNAPSHOT", processingParameters.get("l2ProcessorBundleVersion"));
        assertEquals("BandMaths", processingParameters.get("l2ProcessorName"));
        assertEquals("<!-- no params -->", processingParameters.get("l2ProcessorParameters"));
        assertEquals("1", processingParameters.get("superSampling"));
        assertEquals("NOT INVALID", processingParameters.get("maskExpr"));
        assertNotNull(processingParameters.get("fillValue"));
        assertTrue(Double.isNaN((Double) processingParameters.get("fillValue")));

        // Assert that processing config objects are correct
        BeamL3Config beamL3Config = processingRequest.getBeamL3Config();
        assertNotNull(beamL3Config);
        assertEquals(4320, beamL3Config.getBinningContext().getBinningGrid().getNumRows());
        assertEquals("NOT INVALID", beamL3Config.getVariableContext().getMaskExpr());
        float[] superSamplingSteps = beamL3Config.getSuperSamplingSteps();
        assertEquals(1, superSamplingSteps.length);
        assertEquals(0.5f, superSamplingSteps[0], 1e-5);
        Geometry regionOfInterest = beamL3Config.getRegionOfInterest();
        assertNotNull(regionOfInterest);
        assertEquals("POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))", regionOfInterest.toString());
        assertEquals(3, beamL3Config.getVariableContext().getVariableCount());
        assertEquals("a", beamL3Config.getVariableContext().getVariableName(0));
        assertEquals(" b", beamL3Config.getVariableContext().getVariableName(1));
        assertEquals(" c", beamL3Config.getVariableContext().getVariableName(2));
        BinManager binManager = beamL3Config.getBinningContext().getBinManager();
        assertEquals(3, binManager.getAggregatorCount());
        assertEquals("MIN_MAX", binManager.getAggregator(0).getName());

        FormatterL3Config formatterL3Config = processingRequest.getFormatterL3Config();
        assertNotNull(formatterL3Config);
        assertEquals("NetCDF", formatterL3Config.getOutputFormat());
        assertEquals(new File("/opt/tomcat/webapps/calvalus/staging/ewa-A25F/out/L3_2010-06-03-2010-06-05.nc").getPath(), formatterL3Config.getOutputFile());
        assertEquals("Product", formatterL3Config.getOutputType());
    }

    static ProductionRequest createValidL3ProductionRequest() {
        return new ProductionRequest("calvalus-level3",
                                     // GeneralLevel 3 parameters
                                     "inputProductSetId", "MER_RR__1P/r03/2010",
                                     "outputFileName", "${user}-${id}/out",
                                     "outputFormat", "NetCDF",
                                     "autoStaging", "true",
                                     "l2ProcessorBundleName", "beam",
                                     "l2ProcessorBundleVersion", "4.9-SNAPSHOT",
                                     "l2ProcessorName", "BandMaths",
                                     "l2ProcessorParameters", "<!-- no params -->",
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
                                     "fillValue", "NaN",
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
