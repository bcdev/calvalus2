package com.bc.calvalus.generator;

import com.bc.calvalus.generator.extractor.ReadHistory;

import java.util.Properties;

import static com.bc.calvalus.generator.extractor.Extractor.createProperties;

/**
 * @author muhammad.bc
 */
public class TestUtils {

    private static Properties properties;


    public static String getSaveLocation() {
        if (properties == null) {
            properties = createProperties();
        }
        return properties.getProperty("save.location");
    }

    public static String getJobHistoryURL() {
        if (properties == null) {
            properties = createProperties();
        }
        return properties.getProperty("calvalus.history.jobs.url");
    }


    public static boolean checkConnection() {
        try {
            String jobHistoryURL = TestUtils.getJobHistoryURL();
            ReadHistory readHistory = new ReadHistory(jobHistoryURL);
            return readHistory.isConnect();
        } catch (IllegalArgumentException e) {

        }
        return false;
    }

}
