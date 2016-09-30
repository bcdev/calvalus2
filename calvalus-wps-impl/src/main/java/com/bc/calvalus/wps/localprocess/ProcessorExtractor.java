package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.ceres.binding.BindingException;
import com.bc.wps.utilities.PropertiesWrapper;
import org.apache.commons.io.IOUtils;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class ProcessorExtractor {

    public List<WpsProcess> getProcessors() throws BindingException, IOException, URISyntaxException {
        List<WpsProcess> processors = new ArrayList<>();
        for (BundleDescriptor bundleDescriptor : getLocalBundleDescriptors()) {
            if (bundleDescriptor.getProcessorDescriptors() == null) {
                continue;
            }
            ProcessorDescriptor[] processorDescriptors = bundleDescriptor.getProcessorDescriptors();
            for (ProcessorDescriptor processorDescriptor : processorDescriptors) {
                processors.add(new LocalProcessor(bundleDescriptor, processorDescriptor));
            }
        }
        return processors;
    }

    public WpsProcess getProcessor(ProcessorNameConverter converter)
                throws BindingException, IOException, URISyntaxException {
        for (BundleDescriptor bundleDescriptor : getLocalBundleDescriptors()) {
            if (bundleDescriptor.getProcessorDescriptors() == null) {
                continue;
            }
            if (bundleDescriptor.getBundleName().equals(converter.getBundleName())
                && bundleDescriptor.getBundleVersion().equals(converter.getBundleVersion())) {
                for (ProcessorDescriptor processorDescriptor : bundleDescriptor.getProcessorDescriptors()) {
                    if (processorDescriptor.getExecutableName().equals(converter.getExecutableName())) {
                        return new LocalProcessor(bundleDescriptor, processorDescriptor);
                    }
                }
            }
        }
        return null;
    }

    private List<BundleDescriptor> getLocalBundleDescriptors()
                throws URISyntaxException, IOException, BindingException {
        URL descriptorDirUrl = this.getClass().getResource(PropertiesWrapper.get("utep.descriptor.directory"));
        List<BundleDescriptor> bundleDescriptors = new ArrayList<>();
        if (descriptorDirUrl == null) {
            return bundleDescriptors;
        }
        URI descriptorDirUri = descriptorDirUrl.toURI();
        File descriptorDirectory = Paths.get(descriptorDirUri).toFile();
        File[] descriptorFiles = descriptorDirectory.listFiles();
        if (descriptorFiles == null) {
            return bundleDescriptors;
        }
        for (File descriptorFile : descriptorFiles) {
            FileInputStream fileInputStream = new FileInputStream(descriptorFile);
            String bundleDescriptorXml = IOUtils.toString(fileInputStream);
            ParameterBlockConverter parameterBlockConverter = new ParameterBlockConverter();
            BundleDescriptor bundleDescriptor = new BundleDescriptor();
            parameterBlockConverter.convertXmlToObject(bundleDescriptorXml, bundleDescriptor);
            bundleDescriptors.add(bundleDescriptor);
        }
        return bundleDescriptors;
    }

}
