package com.bc.calvalus.generator;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.generator.extractor.ReadHistory;
import com.bc.wps.utilities.PropertiesWrapper;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;


/**
 * @author muhammad.bc
 */
public class TestUtils {

    private static Properties properties;

    public static String getJobHistoryURL() {

        if (properties == null) {
            try {
                PropertiesWrapper.loadConfigFile("conf/calvalus-reporting.properties");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return PropertiesWrapper.get("calvalus.history.jobs.url");
    }


    public static boolean checkConnection() {
        try {
            String jobHistoryURL = TestUtils.getJobHistoryURL();
            ReadHistory readHistory = new ReadHistory(jobHistoryURL);
            return readHistory.isConnect();
        } catch (IllegalArgumentException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        }
        return false;
    }

}
