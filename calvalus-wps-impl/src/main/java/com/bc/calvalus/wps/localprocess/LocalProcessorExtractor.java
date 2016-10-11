package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.wps.ProcessorExtractor;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
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
class LocalProcessorExtractor extends ProcessorExtractor {

    @Override
    protected BundleDescriptor[] getBundleDescriptors(String userName) throws WpsProcessorNotFoundException {
        URL descriptorDirUrl = this.getClass().getResource(PropertiesWrapper.get("utep.descriptor.directory"));
        List<BundleDescriptor> bundleDescriptors = new ArrayList<>();
        if (descriptorDirUrl == null) {
            descriptorDirUrl = this.getClass().getClassLoader().getResource(PropertiesWrapper.get("utep.descriptor.directory"));
            if (descriptorDirUrl == null) {
                return new BundleDescriptor[0];
            }
        }
        try {
            URI descriptorDirUri = descriptorDirUrl.toURI();
            File descriptorDirectory = Paths.get(descriptorDirUri).toFile();
            File[] descriptorFiles = descriptorDirectory.listFiles();
            if (descriptorFiles == null) {
                return new BundleDescriptor[0];
            }
            for (File descriptorFile : descriptorFiles) {
                FileInputStream fileInputStream = new FileInputStream(descriptorFile);
                String bundleDescriptorXml = IOUtils.toString(fileInputStream);
                ParameterBlockConverter parameterBlockConverter = new ParameterBlockConverter();
                BundleDescriptor bundleDescriptor = new BundleDescriptor();
                parameterBlockConverter.convertXmlToObject(bundleDescriptorXml, bundleDescriptor);
                bundleDescriptors.add(bundleDescriptor);
            }
            return bundleDescriptors.toArray(new BundleDescriptor[bundleDescriptors.size()]);
        } catch (IOException | URISyntaxException | BindingException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
    }
}
