package com.bc.calvalus.wps2.calvalusfacade;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusProductionService;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hans on 13/08/2015.
 */
public class CalvalusProcessorExtractor {

    private static final String DEFAULT_CONFIG_PATH = new File(ProductionServiceConfig.getUserAppDataDir(),
                                                               "calvalus.config").getPath();

    public void getProcessor() throws IOException, ProductionException {
        CalvalusProductionService calvalusProductionService = new CalvalusProductionService();
        CalvalusConfig calvalusConfig = new CalvalusConfig();
        Map<String, String> defaultConfig = calvalusConfig.getDefaultConfig();
        Map<String, String> config = ProductionServiceConfig.loadConfig(new File(DEFAULT_CONFIG_PATH), defaultConfig);
        ProductionService productionService = calvalusProductionService.createProductionService(config);
        BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER");
        BundleDescriptor[] bundleDescriptor = productionService.getBundles("martin", filter);

        System.out.println("bundleDescriptor.length = " + bundleDescriptor.length);
        List<String> processorNames = new ArrayList<>();
        for (BundleDescriptor bundle : bundleDescriptor) {
            System.out.println("bundle.getBundleName() = " + bundle.getBundleName());
            System.out.println("bundle.getBundleLocation() = " + bundle.getBundleLocation());
            System.out.println("bundle.getBundleVersion() = " + bundle.getBundleVersion());
            ProcessorDescriptor[] processorDescriptors = bundle.getProcessorDescriptors();
            if (processorDescriptors == null) {
                continue;
            }
            for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                processorNames.add(processorDescriptor.getProcessorName());
                System.out.println("processorDescriptor.getProcessorName() = " + processorDescriptor.getProcessorName());
                System.out.println("processorDescriptor.getDescriptionHtml() = " + processorDescriptor.getDescriptionHtml());
                System.out.println("processorDescriptor.getProcessorVersion() = " + processorDescriptor.getProcessorVersion());
            }
            System.out.println();
        }
        System.out.println("processorNames.size() = " + processorNames.size());
    }

}
