package com.bc.calvalus.binning;

import com.bc.ceres.glevel.MultiLevelImage;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;


public class ProductSlicingTest {
    @Test
    public void testSlicing() throws IOException {
        System.setProperty("beam.envisat.tileHeight", Integer.toString(64));
        EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        ProductReader productReader = plugIn.createReaderInstance();

        Product sourceProduct = productReader.readProductNodes(new File("testdata/MER_RR__1P_TEST.N1"), null);
        Band band = sourceProduct.getBand("radiance_13");  // todo - use mult. vars, get from config
        MultiLevelImage maskImage = band.getValidMaskImage();
        MultiLevelImage varImage = band.getGeophysicalImage();
        assertThatImageIsSliced(sourceProduct, maskImage);
        assertThatImageIsSliced(sourceProduct, varImage);
    }

    private void assertThatImageIsSliced(Product product, MultiLevelImage image) {
        int tileWidth = image.getTileWidth();
        int sceneRasterWidth = product.getSceneRasterWidth();
        if (tileWidth != sceneRasterWidth) {
            throw new IllegalStateException(MessageFormat.format("Product not sliced: image.tileSize = {0}x{1}, product.sceneRasterSize = {2}x{3}",
                                                                 tileWidth, image.getTileHeight(), sceneRasterWidth, product.getSceneRasterHeight()));
        }
    }
}
