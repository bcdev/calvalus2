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

    static String appDataDir = null;

    public static File getUserAppDataDir() {
        if (appDataDir != null) {
            return new File(appDataDir);
        }
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
        // memorise appDataDir if configured (remains null else)
        appDataDir = calvalusConfig.get("calvalus.portal.appDataDir");

        return calvalusConfig;
    }

    public static void overwriteConfigWithProperties(Map<String, String> calvalusConfig, Properties properties, String prefix) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (prefix != null) {
                if (key.startsWith(prefix)) {
                    calvalusConfig.put(key, value);
                }
            } else {
                calvalusConfig.put(key, value);
            }
        }
    }

    public static Map<String, String> getCalvalusDefaultConfig() {
        Map<String, String> defaultConfig = new HashMap<String, String>();

        defaultConfig.put("calvalus.hadoop.fs.defaultFS", "hdfs://master00:9000");
        defaultConfig.put("calvalus.hadoop.mapreduce.framework.name", "yarn");
        defaultConfig.put("calvalus.hadoop.yarn.resourcemanager.hostname", "master00");
        //defaultConfig.put("calvalus.hadoop.yarn.resourcemanager.address", "master00:9001");
        //defaultConfig.put("calvalus.hadoop.yarn.resourcemanager.scheduler.address", "master00:9002");
        //defaultConfig.put("calvalus.hadoop.yarn.resourcemanager.resource-tracker.address", "master00:9003");

        defaultConfig.put("calvalus.hadoop.dfs.blocksize", "2147483136");
        defaultConfig.put("calvalus.hadoop.io.file.buffer.size", "131072");
        defaultConfig.put("calvalus.hadoop.dfs.replication", "1");

        defaultConfig.put("calvalus.hadoop.mapreduce.map.speculative", "false");
        defaultConfig.put("calvalus.hadoop.mapreduce.reduce.speculative", "false");
        defaultConfig.put("calvalus.hadoop.mapreduce.client.genericoptionsparser.used", "true");

        defaultConfig.put("calvalus.hadoop.dfs.permissions.superusergroup", "hadoop");
        defaultConfig.put("calvalus.hadoop.fs.permissions.umask-mode", "002");
        defaultConfig.put("calvalus.hadoop.yarn.log-aggregation-enable", "true");

        defaultConfig.put("calvalus.hadoop.mapreduce.jobhistory.address", "master00:10200");
        defaultConfig.put("calvalus.hadoop.mapreduce.jobhistory.webapp.address", "master00:19888");

        defaultConfig.put("calvalus.hadoop.yarn.app.mapreduce.am.command-opts", "-Xmx512M -Djava.awt.headless=true");
        defaultConfig.put("calvalus.hadoop.yarn.app.mapreduce.am.resource.mb", "512");
        return defaultConfig;
    }
}
