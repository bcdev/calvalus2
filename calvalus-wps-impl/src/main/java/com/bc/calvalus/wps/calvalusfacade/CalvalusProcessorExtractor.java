package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles the processor lookup operation.
 *
 * @author hans
 */
public class CalvalusProcessorExtractor {

    protected List<IWpsProcess> getProcessors(ProductionService productionService, String userName)
                throws ProductionException {
        BundleDescriptor[] bundleDescriptors = getBundleDescriptors(productionService, userName);

        List<IWpsProcess> processors = new ArrayList<>();
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

    protected CalvalusProcessor getProcessor(ProcessorNameConverter parser, ProductionService productionService, String userName)
                throws ProductionException {
        BundleDescriptor[] bundleDescriptor = getBundleDescriptors(productionService, userName);
        for (BundleDescriptor bundle : bundleDescriptor) {
            if (bundle.getProcessorDescriptors() == null) {
                continue;
            }
            if (bundle.getBundleName().equals(parser.getBundleName())
                && bundle.getBundleVersion().equals(parser.getBundleVersion())) {
                for (ProcessorDescriptor processorDescriptor : bundle.getProcessorDescriptors()) {
                    if (processorDescriptor.getExecutableName().equals(parser.getExecutableName())) {
                        return new CalvalusProcessor(bundle, processorDescriptor);
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
