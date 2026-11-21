package com.bc.calvalus.production.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Map;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusHadoopRequestConverterTest extends TestCase {

    static final String DESCRIPTOR_PARAMETERS = ""
    + "{"
    + "    \"aggregationParameters\" : {"
    + "        \"parameters\": {"
    + "            \"aggregators\": {"
    + "                \"aggregator\": [ {"
    + "                    \"type\": \"TimeChunkedCube\","
    + "                    \"yChunks\": 256,"
    + "                    \"xChunks\": 256,"
    + "                    \"byteOrder\": \"littleendian\","
    + "                    \"compression\": \"zlib,level:1\""
    + "                } ]"
    + "            }"
    + "        }"
    + "    }"
    + "}";

    static final String REQUEST_PARAMETERS = ""
    + "{"
    + "    \"aggregationParameters\" : {"
    + "        \"parameters\": {"
    + "            \"aggregators\": {"
    + "                \"aggregator\": [ {"
    + "                    \"varNames\": \"CHL,CHL_algo,Turbidity,Turbidity_algo,SDD,adg\","
    + "                    \"timeShape\": 272,"
    + "                    \"yShape\": 11640,"
    + "                    \"xShape\": 7780,"
    + "                    \"timeChunks\": 30,"
    + "                    \"yChunks\": 256,"
    + "                    \"xChunks\": 256,"
    + "                    \"cubeMetadata\": \"{\\\"project\\\":\\\"okosat\\\",\\\"creator\\\":\\\"Brockmann Consult GmbH\\\"}\""
    + "                } ]"
    + "            }"
    + "        }"
    + "    }"
    + "}";

    @Test
    public void testMergeFeatures() throws JsonProcessingException {
        final CalvalusHadoopRequestConverter converter = new CalvalusHadoopRequestConverter(null, "cvop");
        final Map<String, Object> descriptor = converter.parseRequest(DESCRIPTOR_PARAMETERS);
        final Map<String, Object> request = converter.parseRequest(REQUEST_PARAMETERS);
        CalvalusHadoopRequestConverter.mergeFeatures(descriptor, request);
        final ObjectMapper jsonParser = new ObjectMapper();
        System.out.println(jsonParser.writer().writeValueAsString(descriptor));
    }

    @Test
    public void testIsRelative() {
        assertTrue(CalvalusHadoopRequestConverter.isRelativePath("test-results/converter"));
        assertFalse(CalvalusHadoopRequestConverter.isRelativePath("/calvalus/home/cvop/test-results/converter"));
        assertFalse(CalvalusHadoopRequestConverter.isRelativePath("s3a://hroc/calvalus/home/cvop/test-results/converter"));
        assertFalse(CalvalusHadoopRequestConverter.isRelativePath("hdfs://calvalus/calvalus/home/cvop/test-results/converter"));
    }
}