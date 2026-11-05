package com.bc.calvalus.production.cli;

import com.bc.calvalus.production.util.DescriptorUtils;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import static org.junit.Assert.*;

public class JsonProductionRequestConverterTest {

    @Test
    public void testConversionToMap() throws Exception {
        String requestPath = getClass().getResource("processing-request.json").getPath();
        final CalvalusHadoopRequestConverter requestConverter =
                new CalvalusHadoopRequestConverter(
                        new CalvalusHadoopConnection("nobody"),
                        "nobody"
                );
        final Map<String, Object> request = requestConverter.parseFeatures(requestPath);
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

    @Test
    public void testConvertParameters() throws IOException, InterruptedException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        final String descriptorAndProductionTypeDir = "calvalus-production/src/test/resources/etc";
        final DescriptorUtils.RoleMatcher roleMatcher = new DescriptorUtils.RoleMatcher("testuser", new String[] { "bc" });
        final CalvalusHadoopRequestConverter converter = new CalvalusHadoopRequestConverter(
                null,
                "testuser",
                roleMatcher,
                descriptorAndProductionTypeDir,
                descriptorAndProductionTypeDir
        );
        Map<String, Object> request = CalvalusHadoopRequestConverter.parseFeatures(
                descriptorAndProductionTypeDir + "/c2rcc-14.0/c2rcc-olci-example-request.json"
        );
        final HashMap<String, String> commandLine = new HashMap<>();
        final HashMap<Object, Object> config = new HashMap<>();
        config.put("calvalus.hadoop.fs.defaultFS", "hdfs://calvalus");
        final CalvalusHadoopParameters hadoopParameters = converter.collectParameters(request, commandLine, config);
        assertEquals(hadoopParameters.get("calvalus.l2.operator"), "c2rcc.olci");
        assertEquals(hadoopParameters.get("fs.defaultFS"), "hdfs://calvalus");
        assertEquals(hadoopParameters.get("mapreduce.map.maxattempts"), "3");
    }
}
