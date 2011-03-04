package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.esa.beam.util.math.DoubleList;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.*;

public class L3ProcessingRequestTest {
    @Test
    public void testGetProcessingParameters() throws ProductionException {

        ProductionRequest productionRequest = createValidL3ProductionRequest();
        L3ProcessingRequestFactory l3ProcessingRequestFactory = new L3ProcessingRequestFactory() {
            @Override
            public String[] getInputFiles(ProductionRequest request) throws ProductionException {
                return new String[]{"F1.N1", "F2.N1", "F3.N1", "F4.N1"};
            }
            @Override
            public String getStagingDir(ProductionRequest request) throws ProductionException {
                return "/";
            }
        };
        L3ProcessingRequest processingRequest = l3ProcessingRequestFactory.createProcessingRequest(productionRequest);

        // Assert that derived processing parameters are generated correctly
        assertEquals(4, processingRequest.getInputFiles().length);
        assertEquals(3, processingRequest.getAggregators().length);
        assertEquals(0, processingRequest.getVariables().length);
        assertEquals("5,50,25,60", processingRequest.getBBox());
        assertEquals(4320, (int) processingRequest.getNumRows());
        assertEquals("calvalus-level3-output", processingRequest.getOutputDir());
        assertEquals("/", processingRequest.getStagingDir());
        assertEquals(true, processingRequest.getOutputStaging());
        assertEquals(true, Double.isNaN(processingRequest.getFillValue()));

        // Assert that derived processing parameters are present in map
        Map<String, Object> processingParameters = processingRequest.getProcessingParameters();
        assertNotNull(processingParameters);
        assertNotNull(processingParameters.get("inputFiles"));
        assertNotNull(processingParameters.get("variables"));
        assertNotNull(processingParameters.get("aggregators"));
        assertEquals("5,50,25,60", processingParameters.get("bbox"));
        assertEquals(4320, processingParameters.get("numRows"));
        assertEquals("calvalus-level3-output", processingParameters.get("outputDir"));
        assertEquals("/", processingParameters.get("stagingDir"));
        assertEquals("id3", processingParameters.get("inputProductSetId"));
        assertEquals("beam", processingParameters.get("l2ProcessorBundleName"));
        assertEquals("4.9-SNAPSHOT", processingParameters.get("l2ProcessorBundleVersion"));
        assertEquals("BandMaths", processingParameters.get("l2ProcessorName"));
        assertEquals("<!-- no params -->", processingParameters.get("l2ProcessorParameters"));
        assertEquals("1", processingParameters.get("superSampling"));
        assertEquals("NOT INVALID", processingParameters.get("maskExpr"));
        assertNotNull(processingParameters.get("fillValue"));
        assertTrue(Double.isNaN((Double) processingParameters.get("fillValue")));
    }

    static ProductionRequest createValidL3ProductionRequest() {
        return new ProductionRequest("calvalus-level3",
                                     // GeneralLevel 3 parameters
                                     "inputProductSetId", "id3",
                                     "outputFileName", "${type}-output",
                                     "outputFormat", "NetCDF",
                                     "outputStaging", "true",
                                     "l2ProcessorBundleName", "beam",
                                     "l2ProcessorBundleVersion", "4.9-SNAPSHOT",
                                     "l2ProcessorName", "BandMaths",
                                     "l2ProcessorParameters", "<!-- no params -->",
                                     // Special Level 3 parameters
                                     "inputVariables", "a, b, c",
                                     "maskExpr", "NOT INVALID",
                                     "aggregator", "MIN_MAX",
                                     "weightCoeff", "1.0",
                                     "dateStart", "2010-06-01",
                                     "dateStop", "2010-06-07",
                                     "periodCount", "1",
                                     "periodLength", "7",
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
