package com.bc.calvalus.rest;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.rest.exceptions.CalvalusProcessorNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class ProcessorExtractor {

    public List<CalvalusProcessor> getProcessors(String userName) throws CalvalusProcessorNotFoundException {
        BundleDescriptor[] bundleDescriptors;
        bundleDescriptors = getBundleDescriptors(userName);

        List<CalvalusProcessor> processors = new ArrayList<>();
        for (BundleDescriptor bundleDescriptor : bundleDescriptors) {
            if (bundleDescriptor.getProcessorDescriptors() == null) {
                continue;
            }
            ProcessorDescriptor[] processorDescriptors = bundleDescriptor.getProcessorDescriptors();
            for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                CalvalusProcessor calvalusProcessor = new CalvalusProcessor(bundleDescriptor, processorDescriptor);
                processors.add(calvalusProcessor);
            }
        }
        return processors;
    }

    public CalvalusProcessor getProcessor(ProcessorNameConverter parser, String userName) throws CalvalusProcessorNotFoundException {
        BundleDescriptor[] bundleDescriptors;
        bundleDescriptors = getBundleDescriptors(userName);
        for (BundleDescriptor bundleDescriptor : bundleDescriptors) {
            if (bundleDescriptor.getProcessorDescriptors() == null) {
                continue;
            }
            if (bundleDescriptor.getBundleName().equals(parser.getBundleName())
                && bundleDescriptor.getBundleVersion().equals(parser.getBundleVersion())) {
                for (ProcessorDescriptor processorDescriptor : bundleDescriptor.getProcessorDescriptors()) {
                    if (processorDescriptor.getExecutableName().equals(parser.getExecutableName())) {
                        return new CalvalusProcessor(bundleDescriptor, processorDescriptor);
                    }
                }
            }
        }
        return null;
    }

    private BundleDescriptor[] getBundleDescriptors(String userName) throws CalvalusProcessorNotFoundException {
        try {
            ProductionService productionService = CalvalusProductionService.getServiceContainerSingleton().getProductionService();
            BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER,ALL_USER").withTheUser(userName);
            return productionService.getBundles(userName, filter);
        } catch (IOException | ProductionException exception) {
            throw new CalvalusProcessorNotFoundException(exception);
        }
    }
}
