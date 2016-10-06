package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class handles the processor lookup operation.
 *
 * @author hans
 */
class CalvalusProcessorExtractor {

    protected List<WpsProcess> getProcessors(String userName) throws WpsProcessorNotFoundException {
        BundleDescriptor[] bundleDescriptors;
        try {
            ProductionService productionService = CalvalusProductionService.getProductionServiceSingleton();
            bundleDescriptors = getBundleDescriptors(productionService, userName);
        } catch (ProductionException | IOException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }

        List<WpsProcess> processors = new ArrayList<>();
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

    protected CalvalusProcessor getProcessor(ProcessorNameConverter parser, String userName) throws WpsProcessorNotFoundException {
        BundleDescriptor[] bundleDescriptors;
        try {
            ProductionService productionService = CalvalusProductionService.getProductionServiceSingleton();
            bundleDescriptors = getBundleDescriptors(productionService, userName);
        } catch (ProductionException | IOException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
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

    private BundleDescriptor[] getBundleDescriptors(ProductionService productionService, String userName)
                throws ProductionException {
        BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER").withTheUser(userName);
        return productionService.getBundles(userName, filter);
    }
}
