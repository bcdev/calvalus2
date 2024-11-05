package com.bc.calvalus.production.cli;

import org.junit.Test;

import java.util.Map;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import static org.junit.Assert.*;

public class JsonProductionRequestConverterTest {

    @Test
    public void testConversionToMap() throws Exception {
        String requestPath = getClass().getResource("processing-request.json").getPath();
        Map<String, Object> request = CalvalusHadoopRequestConverter.parseIntoMap(requestPath);
        for (Map.Entry<String, Object> entry : request.entrySet()) {
            if (entry.getValue() instanceof Map) {
                assertEquals("combinations", entry.getKey());
                XmlMapper xmlMapper = new XmlMapper();
                //final String xml = xmlMapper.writerWithDefaultPrettyPrinter().writeValueAsString(entry.getValue());
                final String xml = xmlMapper.writeValueAsString(entry.getValue());
                final String xmlValue = xml.substring("<LinkedHashMap>".length(), xml.length() - "</LinkedHashMap>".length());
                assertEquals("<parameters>", xmlValue.substring(0, "<parameters>".length()));
            }
        }
    }

    @Test
    public void testConvertJsonRequestWithXml() throws Exception {
        String requestPath = getClass().getResource("processing-request-with-xml-parameters.json").getPath();
        final String jsonRequest = CalvalusHadoopRequestConverter.jsonifyRequest(requestPath);
        assertTrue(jsonRequest.contains("<parameters>"));
        assertTrue(jsonRequest.contains("NUM_TILE_ROWS"));
    }
}
