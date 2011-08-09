package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionRequest;
import org.apache.commons.cli.CommandLine;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import static org.junit.Assert.*;

public class MAToolTest {
    @Test
    public void testCommandLine() throws Exception {
        CommandLine commandLine = new MATool().parseCommandLine("");
        assertNull(commandLine.getOptionValue("request"));
        assertEquals("request.xml", commandLine.getOptionValue("request", "request.xml"));
    }

    @Test
    public void testCommandLineWithOptionR() throws Exception {
        CommandLine commandLine = new MATool().parseCommandLine("-r", "foo.xml");
        assertEquals("foo.xml", commandLine.getOptionValue("request"));
        assertEquals("foo.xml", commandLine.getOptionValue("request", "request.xml"));
    }

    @Test
    public void testConversionToMap() throws Exception {
        InputStream resourceAsStream = getClass().getResourceAsStream("ma-request.xml");
        InputStreamReader inputStreamReader = new InputStreamReader(resourceAsStream);
        ProductionRequest productionRequest = new MATool().loadProductionRequest(inputStreamReader);

        assertNotNull(productionRequest);
        assertEquals("MA", productionRequest.getProductionType());
        assertNotNull(productionRequest.getUserName());
        Map<String, String> parameters = productionRequest.getParameters();
        assertNotNull(parameters);
        assertEquals(8, parameters.size());

        assertTrue(parameters.containsKey("calvalus.processor.package"));
        assertEquals("beam-meris-radiometry", parameters.get("calvalus.processor.package"));

        assertTrue(parameters.containsKey("calvalus.input"));
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_011806_000026382028_00332_12410_0000.N1", parameters.get("calvalus.input"));

        assertTrue(parameters.containsKey("calvalus.l2.parameters"));
        String l2Parameters = parameters.get("calvalus.l2.parameters");
        assertTrue(l2Parameters.startsWith("<parameters>"));
        assertTrue(l2Parameters.endsWith("</parameters>"));
        assertTrue(l2Parameters.contains("<doSmile>true</doSmile>"));
        assertTrue(l2Parameters.contains("<reproVersion>AUTO_DETECT</reproVersion>"));
    }

}
