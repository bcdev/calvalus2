package com.bc.calvalus.production;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Calvalus production service configuration.
 *
 * @author MarcoZ
 * @author Norman
 */
public class ProductionServiceConfig {

    public static File getUserAppDataDir() {
        final String userHome = System.getProperty("user.home");
        return userHome != null ? new File(userHome, ".calvalus") : null;
    }

    public static Map<String, String> loadConfig(File configFile, Map<String, String> defaultConfig) throws IOException {
        Map<String, String> calvalusConfig = defaultConfig != null ? new HashMap<String, String>(defaultConfig) : new HashMap<String, String>();
        FileReader reader = new FileReader(configFile);
        Properties properties = new Properties();
        try {
            properties.load(reader);
            overwriteConfigWithProperties(calvalusConfig, properties, null);
            overwriteConfigWithProperties(calvalusConfig, System.getProperties(), "calvalus.");
        } finally {
            reader.close();
        }
        return calvalusConfig;
    }

    public static void overwriteConfigWithProperties(Map<String, String> calvalusConfig, Properties properties, String prefix) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String name = entry.getKey().toString();
            if (prefix != null) {
                if (name.startsWith(prefix)) {
                    calvalusConfig.put(name, properties.getProperty(name));
                }
            } else {
                calvalusConfig.put(name, properties.getProperty(name));
            }
        }
    }
}
