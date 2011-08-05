package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionRequest;
import org.apache.commons.cli.CommandLine;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

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
        Set<Map.Entry<String,String>> entries = parameters.entrySet();
        assertEquals(8, entries.size());
    }

}
