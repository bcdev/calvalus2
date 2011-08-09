/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

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
        if (configFile != null && configFile.canRead()) {
            FileReader reader = new FileReader(configFile);
            try {
                Properties properties = new Properties();
                properties.load(reader);
                overwriteConfigWithProperties(calvalusConfig, properties, null);
            } finally {
                reader.close();
            }
        }
        overwriteConfigWithProperties(calvalusConfig, System.getProperties(), "calvalus.");
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
