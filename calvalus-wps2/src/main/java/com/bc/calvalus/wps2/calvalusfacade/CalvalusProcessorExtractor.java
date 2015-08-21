package com.bc.calvalus.wps2.calvalusfacade;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps2.Processor;
import com.bc.calvalus.wps2.ProcessorNameParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hans on 13/08/2015.
 */
public class CalvalusProcessorExtractor {

    private ProductionService productionService;

    public CalvalusProcessorExtractor(ProductionService productionService) {
        this.productionService = productionService;
    }

    public List<Processor> getProcessors() throws IOException, ProductionException {
        BundleDescriptor[] bundleDescriptor = getBundleDescriptors();

        List<Processor> processors = new ArrayList<>();
        for (BundleDescriptor bundle : bundleDescriptor) {
            if (bundle.getProcessorDescriptors() == null) {
                continue;
            }
            ProcessorDescriptor[] processorDescriptors = bundle.getProcessorDescriptors();
            for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                Processor processor = new Processor(bundle, processorDescriptor);
                processors.add(processor);
            }
        }
        return processors;
    }

    public Processor getProcessor(ProcessorNameParser parser) throws IOException, ProductionException {
        BundleDescriptor[] bundleDescriptor = getBundleDescriptors();
        for (BundleDescriptor bundle : bundleDescriptor) {
            if (bundle.getProcessorDescriptors() == null) {
                continue;
            }
            if (bundle.getBundleName().equals(parser.getBundleName()) && bundle.getBundleVersion().equals(parser.getBundleVersion())) {
                for (ProcessorDescriptor processorDescriptor : bundle.getProcessorDescriptors()) {
                    if (processorDescriptor.getExecutableName().equals(parser.getExecutableName())) {
                        return new Processor(bundle, processorDescriptor);
                    }
                }
            }
        }
        return null;
    }

    public ProductSet[] getProductSets() throws ProductionException {
        return productionService.getProductSets("hans", "");
    }

    private BundleDescriptor[] getBundleDescriptors() throws ProductionException, IOException {
        BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER");
        return productionService.getBundles("hans", filter);
    }

}
