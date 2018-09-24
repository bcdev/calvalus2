package com.bc.calvalus.commons.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

/**
 * @author hans
 */
public class PropertiesWrapper {

    private static Properties properties;

    public static void loadConfigFile(String fileName) throws IOException {
        URL configFileUrl = PropertiesWrapper.class.getClassLoader().getResource(fileName);
        if (configFileUrl == null) {
            throw new FileNotFoundException("Configuration file '" + fileName + "' cannot be found.");
        }
        File configFile;
        try {
            configFile = new File(configFileUrl.toURI());
        } catch (URISyntaxException exception) {
            throw new FileNotFoundException("Configuration file '" + fileName + "' cannot be found.");
        }
        FileReader fileReader = new FileReader(configFile);
        properties = new Properties();
        properties.load(fileReader);
    }

    public static String get(String key) {
        return properties.getProperty(key);
    }

    public static int getInteger(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    public static long getLong(String key) {
        return Long.parseLong(properties.getProperty(key));
    }

    public static long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            return Long.parseLong(value);
        } else {
            return defaultValue;
        }
    }

    public static Double getDouble(String key) {
        return Double.parseDouble(properties.getProperty(key));
    }

}
