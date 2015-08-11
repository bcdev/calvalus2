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

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.hadoop.HadoopProductionType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.esa.beam.framework.datamodel.Product;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by marcoz on 11.08.15.
 */
public class ShowProductContentMain {

    private static final File USER_APPDATA_DIR = ProductionServiceConfig.getUserAppDataDir();
    private static final File DEFAULT_CONFIG_FILE = new File(USER_APPDATA_DIR, "calvalus.config");

    public static void main(String[] args) throws IOException, ProductionException {
        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        Map<String, String> config = ProductionServiceConfig.loadConfig(DEFAULT_CONFIG_FILE, defaultConfig);

        System.setProperty("path.separator", ":");
        String archiveRootDir = config.get("calvalus.portal.archiveRootDir");
        if (archiveRootDir == null) {
            archiveRootDir = "eodata";
        }
        Configuration conf = new Configuration();
        HadoopProductionType.setJobConfig(config, conf);
        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsInventoryService inventoryService = new HdfsInventoryService(jobClientsMap, archiveRootDir);

        for (String arg : args) {
            System.out.println("====================================================================");
            System.out.println("arg path = " + arg);

            InputPathResolver inputPathResolver = new InputPathResolver();
            inputPathResolver.setMinDate(null);
            inputPathResolver.setMaxDate(null);
            inputPathResolver.setRegionName(null);
            List<String> inputPatterns = inputPathResolver.resolve(arg);
            List<String> firstPattern = new ArrayList<>();
            String pattern = inputPatterns.get(0);
            System.out.println("patterns = " + inputPatterns);
            System.out.println("first pattern = " + pattern);
            firstPattern.add(pattern);
            try {
                FileStatus[] fileStatuses = inventoryService.globFileStatuses(jobClientsMap.getFileSystem("marcoz"), firstPattern);
                System.out.println("fileStatuses.length = " + fileStatuses.length);
                if (fileStatuses.length > 0) {
                    System.out.println("fileStatuses[0] = " + fileStatuses[0]);
                    Path path = fileStatuses[0].getPath();

                    Product product = CalvalusProductIO.readProduct(path, conf, null);
                    if (product != null) {
                        System.out.println("product.getProductReader() = " + product.getProductReader());
                        String b = String.join(",", product.getBandNames());
                        String t = String.join(",", product.getTiePointGridNames());
                        System.out.println(b + "," + t);
                        product.dispose();
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}
