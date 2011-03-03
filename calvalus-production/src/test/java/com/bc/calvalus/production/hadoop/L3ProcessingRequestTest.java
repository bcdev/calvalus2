package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class L3ProcessingRequestTest {
    @Test
    public void testGetProcessingParameters() throws ProductionException {

        ProductionRequest productionRequest = createValidL3ProductionRequest();

        L3ProcessingRequest processingRequest = new L3ProcessingRequest(productionRequest) {
            @Override
            public String[] getInputFiles() throws ProductionException {
                return new String[]{"F1.N1", "F2.N1", "F3.N1", "F4.N1"};
            }
        };

        // Assert that derived processing parameters are generated correctly
        assertEquals(4, processingRequest.getInputFiles().length);
        assertEquals(3, processingRequest.getAggregators().length);
        assertEquals(0, processingRequest.getVariables().length);
        assertEquals("5,50,25,60", processingRequest.getBBox());
        assertEquals(4320, processingRequest.getNumRows());
        assertEquals("calvalus-level3-output", processingRequest.getOutputFileName());

        // Assert that derived processing parameters are present in map
        Map<String,Object> processingParameters = processingRequest.getProcessingParameters();
        assertNotNull(processingParameters);
        assertNotNull(processingParameters.get("inputFiles"));
        assertNotNull(processingParameters.get("variables"));
        assertNotNull(processingParameters.get("aggregators"));
        assertEquals("5,50,25,60", processingParameters.get("bbox"));
        assertEquals(4320,  processingParameters.get("numRows"));
        assertEquals("calvalus-level3-output", processingParameters.get("outputDir"));
        assertEquals("1234", processingParameters.get("productionId"));
        assertEquals("Wonderful L3", processingParameters.get("productionName"));
        assertEquals("beam", processingParameters.get("l2ProcessorBundleName"));
        assertEquals("4.9-SNAPSHOT", processingParameters.get("l2ProcessorBundleVersion"));
        assertEquals("BandMaths", processingParameters.get("l2ProcessorName"));
        assertEquals("<!-- no params -->", processingParameters.get("l2ProcessorParameters"));
        assertEquals("1", processingParameters.get("superSampling"));
        assertEquals("NOT INVALID", processingParameters.get("maskExpr"));
    }

    static ProductionRequest createValidL3ProductionRequest() {
        return new ProductionRequest("calvalus-level3",
                                                                        // GeneralLevel 3 parameters
                                                                        "productionName", "Wonderful L3",
                                                                        "inputProductSetId", "id3",
                                                                        "outputFileName", "${type}-output",
                                                                        "outputFormat", "NetCDF",
                                                                        "outputStaging", "true",
                                                                        "l2ProcessorBundleName", "beam",
                                                                        "l2ProcessorBundleVersion", "4.9-SNAPSHOT",
                                                                        "l2ProcessorName", "BandMaths",
                                                                        "l2ProcessorParameters", "<!-- no params -->",
                                                                        // Special Level 3 parameters
                                                                        "productionId", "1234",
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
                                                                        "superSampling", "1"
            );
    }


}
