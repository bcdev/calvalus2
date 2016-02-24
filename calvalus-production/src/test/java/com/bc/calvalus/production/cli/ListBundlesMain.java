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

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.AggregatorDescriptor;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
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
public class ListBundlesMain {

    private static final File USER_APPDATA_DIR = ProductionServiceConfig.getUserAppDataDir();
    private static final File DEFAULT_CONFIG_FILE = new File(USER_APPDATA_DIR, "calvalus.config");

    public static void main(String[] args) throws IOException, ProductionException {
        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        Map<String, String> config = ProductionServiceConfig.loadConfig(DEFAULT_CONFIG_FILE, defaultConfig);
        HadoopProductionServiceFactory productionServiceFactory = new HadoopProductionServiceFactory();
        ProductionService productionService = productionServiceFactory.create(config, USER_APPDATA_DIR, new File("."));

        printBundles("system", productionService.getBundles("marcoz", new BundleFilter().withProvider(BundleFilter.PROVIDER_SYSTEM)));
        printDivider();
        printBundles("users", productionService.getBundles("marcoz", new BundleFilter().withProvider(BundleFilter.PROVIDER_ALL_USERS)));
        printDivider();
        printProductSets(productionService.getProductSets("marcoz", "*"));

        productionService.close();
    }

    private static void printDivider() {
        System.out.println();
        System.out.println("================================================================");
        System.out.println();
    }

    private static void printProductSets(ProductSet[] productSets) {
        System.out.println("ProductSet count: " + productSets.length);
        for (ProductSet productSet : productSets) {
            System.out.println(productSet.getName() + " = " + productSet.getPath());
        }
    }

    public static void printBundles(String provider, BundleDescriptor[] bundles) {
        System.out.println(provider + " Bundle count: " + bundles.length);
        for (BundleDescriptor bundle : bundles) {
            System.out.println("Bundle = " + bundle.getBundleLocation() + " (" + bundle.getOwner() + ")");
            AggregatorDescriptor[] aggregatorDescriptors = bundle.getAggregatorDescriptors();
            ProcessorDescriptor[] processorDescriptors = bundle.getProcessorDescriptors();

            if (!bundle.getBundleLocation().endsWith("/" + bundle.getBundleName() + "-" + bundle.getBundleVersion())) {
                System.out.println("WARNING: bundle names and version don't match directory");
            }

            if (aggregatorDescriptors != null) {
                System.out.println(bundle.getBundleName() + " - " + bundle.getBundleVersion() + " : " + bundle.getBundleLocation());
                for (AggregatorDescriptor aggregatorDescriptor : aggregatorDescriptors) {
                    System.out.println("  Aggregator = " + aggregatorDescriptor.getAggregator());
                }
            }

            if (processorDescriptors != null) {
                for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                    System.out.println("  Processor = " + processorDescriptor.getProcessorName() + " [version " + processorDescriptor.getProcessorVersion() + "]");
                }
            }
        }
    }
}
