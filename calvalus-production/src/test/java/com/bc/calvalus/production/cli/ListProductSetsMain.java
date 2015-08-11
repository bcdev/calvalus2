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

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.hadoop.HadoopProductionServiceFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by marcoz on 07.08.15.
 */
public class ListProductSetsMain {

    private static final File USER_APPDATA_DIR = ProductionServiceConfig.getUserAppDataDir();
    private static final File DEFAULT_CONFIG_FILE = new File(USER_APPDATA_DIR, "calvalus.config");

    public static void main(String[] args) throws IOException, ProductionException {
        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        Map<String, String> config = ProductionServiceConfig.loadConfig(DEFAULT_CONFIG_FILE, defaultConfig);
        HadoopProductionServiceFactory productionServiceFactory = new HadoopProductionServiceFactory();
        ProductionService productionService = productionServiceFactory.create(config, USER_APPDATA_DIR, new File("."));


        ProductSet[] productSets = productionService.getProductSets("marcoz", null);
        printProductsets(productSets);

        productionService.close();
    }

    private static void printProductsets(ProductSet[] productSets) {
        System.out.println("productSets.length = " + productSets.length);

        int maxNameLength = 0;
        int maxTypeLength = 0;
        for (ProductSet productSet : productSets) {
            maxNameLength = Math.max(maxNameLength, productSet.getName().length());
            maxTypeLength = Math.max(maxTypeLength, productSet.getProductType().length());
        }
        String format = String.format("%%-%ds | %%-%ds | %%-15s | %%s%%n", maxTypeLength, maxNameLength);
        System.out.println("format = " + format);
        for (ProductSet productSet : productSets) {
            int numBands = productSet.getBandNames().length;
            System.out.printf("%2d | " + format, numBands, productSet.getProductType(), productSet.getName(), productSet.getRegionName(), productSet.getPath());
        }
    }

}
