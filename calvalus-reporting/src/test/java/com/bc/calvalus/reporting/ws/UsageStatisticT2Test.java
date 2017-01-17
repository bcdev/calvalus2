package com.bc.calvalus.reporting.ws;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.io.JSONExtractor;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.*;

import java.util.List;

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

        assertThat(usageStatisticT2.getId(), equalTo("bc_job_1481485063251_20113"));
        assertThat(usageStatisticT2.getAccountPlatform(), equalTo("Brockmann Consult Processing Center"));
        assertThat(usageStatisticT2.getAccountUserName(), equalTo("cvop"));
        assertThat(usageStatisticT2.getAccountRef(), equalTo("DUMMY-xx-20170101"));
        assertThat(usageStatisticT2.getCompoundId(), equalTo("DUMMY-20170116_11212222"));
        assertThat(usageStatisticT2.getCompoundName(), equalTo("Subsetting of Urban Mask"));
        assertThat(usageStatisticT2.getCompoundType(), equalTo("Subset"));
        assertThat(usageStatisticT2.getHostName(), equalTo("www.brockmann-consult.de/bc-wps/wps/calvalus"));
        assertThat(usageStatisticT2.getStatus(), equalTo("SUCCEEDED"));

        List<Quantity> quantity = usageStatisticT2.getQuantity();
        assertThat(quantity.size(), equalTo(6));
        assertThat(quantity.get(0).getId(), equalTo("CPU_MILLISECONDS"));
        assertThat(quantity.get(0).getValue(), equalTo(380040L));
        assertThat(quantity.get(1).getId(), equalTo("PHYSICAL_MEMORY_BYTES"));
        assertThat(quantity.get(1).getValue(), equalTo(825553920L));
        assertThat(quantity.get(2).getId(), equalTo("BYTE_READ"));
        assertThat(quantity.get(2).getValue(), equalTo(3432637L));
        assertThat(quantity.get(3).getId(), equalTo("BYTE_WRITTEN"));
        assertThat(quantity.get(3).getValue(), equalTo(319233L));
        assertThat(quantity.get(4).getId(), equalTo("PROC_INSTANCE"));
        assertThat(quantity.get(4).getValue(), equalTo(322482L));
        assertThat(quantity.get(5).getId(), equalTo("NUM_REQ"));
        assertThat(quantity.get(5).getValue(), equalTo(1L));

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