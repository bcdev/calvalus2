package com.bc.calvalus.reporting.io;

import com.bc.calvalus.reporting.ws.UsageStatistic;
import com.bc.wps.utilities.PropertiesWrapper;

import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
public class JSONReader {

    public JSONReader() throws IOException {
        PropertiesWrapper.loadConfigFile("calvalus-reporting.properties");
    }

    public UsageStatistic getSingleStatistic(String jobId) {
        return null;
    }

    public List<UsageStatistic> getAllStatistics() {
        return null;
    }
}
