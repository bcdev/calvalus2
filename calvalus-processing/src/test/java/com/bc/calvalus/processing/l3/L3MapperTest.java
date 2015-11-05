package com.bc.calvalus.processing.l3;

import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Test;

import static org.junit.Assert.*;

public class L3MapperTest {

    @Test
    public void testExtractProcessingGraphXml() {
        final Product product = new Product("bla", "blub", 3, 3);


        final MetadataElement processingGraph = new MetadataElement("Processing_Graph");
        final MetadataAttribute metadataAttribute = new MetadataAttribute("test_attrib", ProductData.createInstance(new double[]{
                1.98
        }), true);
        processingGraph.addAttribute(metadataAttribute);

        product.getMetadataRoot().addElement(processingGraph);

        final String metadataXml = L3Mapper.extractProcessingGraphXml(product);
        assertEquals(2259, metadataXml.length());
        assertTrue(metadataXml.contains("1.98"));

    }
}
