package com.bc.calvalus.reporting.collector;

import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.junit.*;

import java.io.InputStream;

/**
 * @author hans
 */
public class HistoryServerClientTest {

    private Gson gson;

    private HistoryServerClient serverClient;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("reporting-collector-test.properties");
        serverClient = new HistoryServerClient();
        gson = new Gson();
    }

    @Ignore //because the returned results are not always the same
    @Test
    public void canGetAllJobs() throws Exception {
        InputStream allJobsStream = serverClient.getAllJobs();
        String allJobsString = IOUtils.toString(allJobsStream);

        System.out.println(allJobsString);
    }

    @Ignore //because the returned results are not always the same
    @Test
    public void canGetConf() throws Exception {
        InputStream confStream = serverClient.getConf("job_1498650116199_0092");
        String confString = IOUtils.toString(confStream);

        System.out.println(confString);
    }

    @Ignore //because the returned results are not always the same
    @Test
    public void canGetCounters() throws Exception {
        InputStream countersStream = serverClient.getCounters("job_1498650116199_0092");
        String countersString = IOUtils.toString(countersStream);

        System.out.println(countersString);
    }
}