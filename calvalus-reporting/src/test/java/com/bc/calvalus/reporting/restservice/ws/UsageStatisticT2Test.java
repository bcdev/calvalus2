package com.bc.calvalus.reporting.restservice.ws;

import com.bc.calvalus.reporting.restservice.io.JSONExtractor;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author hans
 */
public class UsageStatisticT2Test {

    private JSONExtractor jsonExtractor;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-reporting.properties");
        jsonExtractor = new JSONExtractor();

    }

    @Ignore("ignored because a new change in JSONExtractor broke the loading data mechanism (database path hardcoded to local personal dir)")
    @Test
    public void canGetUsageStatisticT2() throws Exception {
        UsageStatistic usageStatistic = jsonExtractor.getSingleStatistic("job_1484434434570_0374", "2017-01-10");

        UsageStatisticT2 usageStatisticT2 = new UsageStatisticT2(usageStatistic);

        assertThat(usageStatisticT2.getId(), equalTo("job_1484434434570_0374"));
        assertThat(usageStatisticT2.getAccountPlatform(), equalTo("Brockmann Consult Processing Center"));
        assertThat(usageStatisticT2.getAccountUserName(), equalTo("tep_hans"));
        assertThat(usageStatisticT2.getAccountRef(), equalTo("1738ad7b-534e-4aca-9861-b26fb9c0f983"));
        assertThat(usageStatisticT2.getCompoundId(), equalTo("20170203115536_L2Plus_caa2e0dbce2c"));
        assertThat(usageStatisticT2.getCompoundName(), equalTo("TEP Subset test"));
        assertThat(usageStatisticT2.getCompoundType(), equalTo("Subset"));
        assertThat(usageStatisticT2.getHostName(), equalTo("www.brockmann-consult.de"));
        assertThat(usageStatisticT2.getStatus(), equalTo("SUCCEEDED"));

        List<Quantity> quantity = usageStatisticT2.getQuantity();
        assertThat(quantity.size(), equalTo(6));
        assertThat(quantity.get(0).getId(), equalTo("CPU_MILLISECONDS"));
        assertThat(quantity.get(0).getValue(), equalTo(23240L));
        assertThat(quantity.get(1).getId(), equalTo("PHYSICAL_MEMORY_BYTES"));
        assertThat(quantity.get(1).getValue(), equalTo(18826240L));
        assertThat(quantity.get(2).getId(), equalTo("BYTE_READ"));
        assertThat(quantity.get(2).getValue(), equalTo(2989591L));
        assertThat(quantity.get(3).getId(), equalTo("BYTE_WRITTEN"));
        assertThat(quantity.get(3).getValue(), equalTo(325908L));
        assertThat(quantity.get(4).getId(), equalTo("PROC_INSTANCE"));
        assertThat(quantity.get(4).getValue(), equalTo(18385L));
        assertThat(quantity.get(5).getId(), equalTo("NUM_REQ"));
        assertThat(quantity.get(5).getValue(), equalTo(1L));

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String usageStatisticJson = gson.toJson(usageStatisticT2);

        assertThat(usageStatisticJson, containsString("{\n" +
                "  \"id\": \"job_1484434434570_0374\",\n" +
                "  \"accountPlatform\": \"Brockmann Consult Processing Center\",\n" +
                "  \"accountUserName\": \"tep_hans\",\n" +
                "  \"accountRef\": \"1738ad7b-534e-4aca-9861-b26fb9c0f983\",\n" +
                "  \"compoundId\": \"20170203115536_L2Plus_caa2e0dbce2c\",\n" +
                "  \"compoundName\": \"TEP Subset test\",\n" +
                "  \"compoundType\": \"Subset\",\n" +
                "  \"quantity\": [\n" +
                "    {\n" +
                "      \"id\": \"CPU_MILLISECONDS\",\n" +
                "      \"value\": 23240\n" +
                "    },\n" +
                "    {\n" +
                "      \"id\": \"PHYSICAL_MEMORY_BYTES\",\n" +
                "      \"value\": 18826240\n" +
                "    },\n" +
                "    {\n" +
                "      \"id\": \"BYTE_READ\",\n" +
                "      \"value\": 2989591\n" +
                "    },\n" +
                "    {\n" +
                "      \"id\": \"BYTE_WRITTEN\",\n" +
                "      \"value\": 325908\n" +
                "    },\n" +
                "    {\n" +
                "      \"id\": \"PROC_INSTANCE\",\n" +
                "      \"value\": 18385\n" +
                "    },\n" +
                "    {\n" +
                "      \"id\": \"NUM_REQ\",\n" +
                "      \"value\": 1\n" +
                "    }\n" +
                "  ],\n" +
                "  \"hostName\": \"www.brockmann-consult.de\",\n" +
                "  \"timeStamp\": \""));

        assertThat(usageStatisticJson, containsString("  \"status\": \"SUCCEEDED\"\n" +
                "}"));
    }
}