package com.bc.calvalus.production.cli;

import com.bc.calvalus.production.ProductionRequest;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import static org.junit.Assert.*;

public class WpsProductionRequestConverterTest {

    @Test
    public void testConversionToMap() throws Exception {
        InputStream resourceAsStream = getClass().getResourceAsStream("ma-request.xml");
        InputStreamReader inputStreamReader = new InputStreamReader(resourceAsStream);
        ProductionRequest productionRequest = new WpsProductionRequestConverter(inputStreamReader).loadProductionRequest("testuser");

        assertNotNull(productionRequest);
        assertEquals("MA", productionRequest.getProductionType());
        assertNotNull(productionRequest.getUserName());
        assertEquals("testuser", productionRequest.getUserName());
        Map<String, String> parameters = productionRequest.getParameters();
        assertNotNull(parameters);
        assertEquals(9, parameters.size());

        assertTrue(parameters.containsKey("calvalus.processor.package"));
        assertEquals("beam-meris-radiometry", parameters.get("calvalus.processor.package"));

        assertTrue(parameters.containsKey("calvalus.input"));

        String inputValue = parameters.get("calvalus.input");
        assertTrue(inputValue.contains(","));
        String[] splits = inputValue.split(",");
        assertEquals(2, splits.length);
        assertEquals("hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_011806_000026382028_00332_12410_0000.N1", splits[0]);
        assertEquals("hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_021806_000026382028_00332_12410_0000.N1", splits[1]);

        assertTrue(parameters.containsKey("calvalus.l2.parameters"));
        String l2Parameters = parameters.get("calvalus.l2.parameters");
        assertTrue(l2Parameters.startsWith("<parameters>"));
        assertTrue(l2Parameters.endsWith("</parameters>"));
        assertTrue(l2Parameters.contains("<doSmile>true</doSmile>"));
        assertTrue(l2Parameters.contains("<reproVersion>AUTO_DETECT</reproVersion>"));

        assertTrue(parameters.containsKey("calvalus.output.compression"));
        assertNotNull(parameters.get("calvalus.output.compression"));
        assertEquals("", parameters.get("calvalus.output.compression"));

        assertNull(productionRequest.getRegionGeometry(null));

    }

}
