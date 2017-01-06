package com.bc.calvalus.generator;

import com.bc.calvalus.generator.reader.LogReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Author ubits on 1/3/2017.
 */
public class TestUtils {

    private static Properties properties;
    private static TestUtils instance;

    private static void createProperties() {
        try (InputStream resourceAsStream = TestUtils.class.getClassLoader().getResourceAsStream("./conf/calvalus-reporting.properties")) {
            properties = new Properties();
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getSaveLocation() {
        if (properties == null) {
            createProperties();
        }
        return properties.getProperty("save.location");
    }

    public static String getJobHistoryURL() {
        if (properties == null) {
            createProperties();
        }
        return properties.getProperty("calvalus.history.url");
    }

    public static boolean checkConnection() {
        try {
            String jobHistoryURL = TestUtils.getJobHistoryURL();
            LogReader logReader = new LogReader(jobHistoryURL);
            return logReader.isConnect();
        } catch (IllegalArgumentException e) {

        }
        return false;
    }

}
