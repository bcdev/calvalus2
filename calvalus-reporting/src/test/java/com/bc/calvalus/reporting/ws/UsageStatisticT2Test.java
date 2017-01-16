package com.bc.calvalus.reporting.ws;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.io.JSONExtractor;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.*;

/**
 * @author hans
 */
public class UsageStatisticT2Test {

    private JSONExtractor jsonExtractor;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
        jsonExtractor = new JSONExtractor();

    }

    @Test
    public void canGetUsageStatisticT2() throws Exception {
        UsageStatistic usageStatistic = jsonExtractor.getSingleStatistic("job_1481485063251_20113");

        UsageStatisticT2 usageStatisticT2 = new UsageStatisticT2(usageStatistic);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String usageStatisticJson = gson.toJson(usageStatisticT2);

        assertThat(usageStatisticJson, containsString("{\n" +
                                                      "  \"id\": \"bc_job_1481485063251_20113\",\n" +
                                                      "  \"accountPlatform\": \"Brockmann Consult Processing Center\",\n" +
                                                      "  \"accountUserName\": \"cvop\",\n" +
                                                      "  \"accountRef\": \"DUMMY-xx-20170101\",\n" +
                                                      "  \"compoundId\": \"DUMMY-20170116_11212222\",\n" +
                                                      "  \"compoundName\": \"Subsetting of Urban Mask\",\n" +
                                                      "  \"compoundType\": \"Subset\",\n" +
                                                      "  \"quantity\": [\n" +
                                                      "    {\n" +
                                                      "      \"id\": \"CPU_MILLISECONDS\",\n" +
                                                      "      \"value\": 380040\n" +
                                                      "    },\n" +
                                                      "    {\n" +
                                                      "      \"id\": \"PHYSICAL_MEMORY_BYTES\",\n" +
                                                      "      \"value\": 825553920\n" +
                                                      "    },\n" +
                                                      "    {\n" +
                                                      "      \"id\": \"BYTE_READ\",\n" +
                                                      "      \"value\": 3432637\n" +
                                                      "    },\n" +
                                                      "    {\n" +
                                                      "      \"id\": \"BYTE_WRITTEN\",\n" +
                                                      "      \"value\": 319233\n" +
                                                      "    },\n" +
                                                      "    {\n" +
                                                      "      \"id\": \"PROC_INSTANCE\",\n" +
                                                      "      \"value\": 322482\n" +
                                                      "    },\n" +
                                                      "    {\n" +
                                                      "      \"id\": \"NUM_REQ\",\n" +
                                                      "      \"value\": 1\n" +
                                                      "    }\n" +
                                                      "  ],\n" +
                                                      "  \"hostName\": \"www.brockmann-consult.de/bc-wps/wps/calvalus\",\n" +
                                                      "  \"timeStamp\": \""));

        assertThat(usageStatisticJson, containsString("  \"status\": \"SUCCEEDED\"\n" +
                                               "}"));
    }
}