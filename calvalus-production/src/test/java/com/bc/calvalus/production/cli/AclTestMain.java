/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.production.cli;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionServiceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

/**
 * @author boe
 */
public class AclTestMain {

    private static final File USER_APPDATA_DIR = ProductionServiceConfig.getUserAppDataDir();
    private static final File DEFAULT_CONFIG_FILE = new File(USER_APPDATA_DIR, "calvalus.config");

    public static void main(String[] args) throws IOException, ProductionException, InterruptedException {
        final Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        final Map<String, String> config = ProductionServiceConfig.loadConfig(DEFAULT_CONFIG_FILE, defaultConfig);
        final UserGroupInformation martin = UserGroupInformation.createRemoteUser("martin");
        FileSystem fs = martin.doAs(new PrivilegedExceptionAction<FileSystem>() {
            @Override
            public FileSystem run() throws Exception {
                Configuration hadoopConfig = getHadoopConf(config);
                return FileSystem.get(hadoopConfig);
            }
        });
        final FileStatus status = fs.getFileStatus(new Path("/calvalus/home/martin/test/three"));
        final AclStatus aclStatus = fs.getAclStatus(new Path("/calvalus/home/martin/test/three"));
        System.out.println(status.toString());
        System.out.println(aclStatus.toString());
        try {
            System.out.println(fs.exists(new Path("/calvalus/home/martin/test/one/${yyyy}/${MM}/${dd}/.*.N1")));
        } catch (AccessControlException e) {
            System.out.println(e.getMessage());
        }
        try {
            System.out.println(fs.exists(new Path("/calvalus/home/martin/test/three/x")));
        } catch (AccessControlException e) {
            System.out.println("|" + e.getMessage() + "|");
        }
//        FileStatus[] fileStatuses = fs.globStatus(new Path("/calvalus/home/martin/test/*/mist"));
//        for (FileStatus s : fileStatuses) {
//            System.out.println(s.toString());
//        }
    }

    private static Configuration getHadoopConf(Map<String, String> config) {
        Configuration hadoopConfig = new Configuration();
        for (String key : config.keySet()) {
            if (key.startsWith("calvalus.hadoop.")) {
                hadoopConfig.set(key.substring("calvalus.hadoop.".length()), config.get(key));
            }
        }
        return hadoopConfig;
    }
}
