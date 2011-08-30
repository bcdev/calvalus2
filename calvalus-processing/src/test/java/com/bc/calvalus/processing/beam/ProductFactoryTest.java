package com.bc.calvalus.processing.beam;

import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.junit.Test;

import static org.junit.Assert.assertSame;

/**
 * @author Norman
 */
public class ProductFactoryTest {
    @Test
    public void testChainIsSetUp() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product sourceProduct = new Product("N", "T", 100, 100);
        sourceProduct.addBand("b1", "1.0");
        sourceProduct.addBand("b2", "2.0");

        Product targetProduct = ProductFactory.getProcessedProduct(sourceProduct, "", "PassThrough", "<parameters/>");
        assertSame(sourceProduct, targetProduct);
    }
}
