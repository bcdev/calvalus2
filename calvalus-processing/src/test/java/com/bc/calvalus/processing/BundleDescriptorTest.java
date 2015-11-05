package com.bc.calvalus.processing;

import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;
import org.esa.snap.core.util.io.FileUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.*;

public class BundleDescriptorTest {


    @Test
    public void testBundleDescriptorCreationByXML() throws Exception {
        String xml = getResourceAsString("test1-bundle-descriptor.xml");
        BundleDescriptor bundleDescriptor = new BundleDescriptor();

        assertNull(bundleDescriptor.getBundleName());

        new ParameterBlockConverter().convertXmlToObject(xml, bundleDescriptor);
        assertEquals("bundle-test-name", bundleDescriptor.getBundleName());
        assertEquals("1.1-SNAPSHOT", bundleDescriptor.getBundleVersion());

        final ProcessorDescriptor[] processorDescriptors = bundleDescriptor.getProcessorDescriptors();
        assertNotNull(processorDescriptors);
        assertEquals(2, processorDescriptors.length);
        final ProcessorDescriptor processorDescriptor1 = processorDescriptors[0];
        assertEquals("executeMe.bat", processorDescriptor1.getExecutableName());
        assertEquals("Science Test Processor", processorDescriptor1.getProcessorName());

        final ProcessorDescriptor processorDescriptor2 = processorDescriptors[1];
        assertEquals("run.exe", processorDescriptor2.getExecutableName());
        assertEquals("Do Test Processing", processorDescriptor2.getProcessorName());

    }

    private String getResourceAsString(String name) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(name));
        try {
            String text = FileUtils.readText(inputStreamReader);
            return text.trim();
        } finally {
            inputStreamReader.close();
        }
    }
}
