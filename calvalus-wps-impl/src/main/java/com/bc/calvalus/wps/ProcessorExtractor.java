package com.bc.calvalus.wps;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.wps.calvalusfacade.CalvalusProcessor;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public abstract class ProcessorExtractor {

    public List<WpsProcess> getProcessors(String userName) throws WpsProcessorNotFoundException {
        BundleDescriptor[] bundleDescriptors;
        bundleDescriptors = getBundleDescriptors(userName);

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

    public WpsProcess getProcessor(ProcessorNameConverter parser, String userName) throws WpsProcessorNotFoundException {
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

    protected abstract BundleDescriptor[] getBundleDescriptors(String userName) throws WpsProcessorNotFoundException;
}
