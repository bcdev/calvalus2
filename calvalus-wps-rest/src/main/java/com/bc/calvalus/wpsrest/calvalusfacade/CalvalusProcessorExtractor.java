package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.CalvalusProcessor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.responses.IWpsProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class handles the processor lookup operation.
 * <p/>
 * Created by hans on 13/08/2015.
 */
public class CalvalusProcessorExtractor {

    private ProductionService productionService;
    private String userName;

    protected CalvalusProcessorExtractor(ProductionService productionService, String userName) {
        this.productionService = productionService;
        this.userName = userName;
    }

    protected List<IWpsProcess> getProcessors() throws IOException, ProductionException {
        BundleDescriptor[] bundleDescriptors = getBundleDescriptors();

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

    protected CalvalusProcessor getProcessor(ProcessorNameParser parser) throws IOException, ProductionException {
        BundleDescriptor[] bundleDescriptor = getBundleDescriptors();
        for (BundleDescriptor bundle : bundleDescriptor) {
            if (bundle.getProcessorDescriptors() == null) {
                continue;
            }
            if (bundle.getBundleName().equals(parser.getBundleName()) && bundle.getBundleVersion().equals(parser.getBundleVersion())) {
                for (ProcessorDescriptor processorDescriptor : bundle.getProcessorDescriptors()) {
                    if (processorDescriptor.getExecutableName().equals(parser.getExecutableName())) {
                        return new CalvalusProcessor(bundle, processorDescriptor);
                    }
                }
            }
        }
        return null;
    }

    private BundleDescriptor[] getBundleDescriptors() throws ProductionException, IOException {
        BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER");
        return productionService.getBundles(userName, filter);
    }
}
