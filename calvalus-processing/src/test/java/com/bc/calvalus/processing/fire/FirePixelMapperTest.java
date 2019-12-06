package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Before;
import org.junit.Test;

import java.awt.Rectangle;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FirePixelMapperTest {

    private Product product;

    @Before
    public void setUp() throws Exception {
        product = ProductIO.readProduct(getClass().getResource("OLCIMODIS2019_1_h19v10_Classification.tif").getFile());
    }

    @Test
    public void testMapData() throws IOException {
        short[] classificationPixels = new short[200];
        FirePixelMapper.readData(product, new Rectangle(1193, 0, 200, 1), classificationPixels);
        assertEquals(-2, classificationPixels[0]);
        assertEquals(0, classificationPixels[7]);
        assertEquals(-1, classificationPixels[95]);
        assertEquals(28, classificationPixels[163]);
        assertEquals(28, classificationPixels[164]);
    }

    @Test
    public void testGetBinIndex() {
        assertEquals(4665668400L, FirePixelMapper.getBinIndex(product, new Rectangle(0, 0, 1, 1), 64800));
        assertEquals(4665668400L + 2 * 129600 + 2, FirePixelMapper.getBinIndex(product, new Rectangle(2, 2, 1, 1), 64800));
        assertEquals(4665668400L + 3599 * 129600 + 3599, FirePixelMapper.getBinIndex(product, new Rectangle(3599, 3599, 1, 1), 64800));
    }
}