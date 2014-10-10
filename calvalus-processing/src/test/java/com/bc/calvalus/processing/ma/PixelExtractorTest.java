package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.Product;
import org.junit.Test;

import java.awt.geom.AffineTransform;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author MarcoZ
 * @author Norman
 */
public class PixelExtractorTest {

    @Test(expected = NullPointerException.class)
    public void testExtractDoesNotAcceptNullRecord() throws Exception {
        PixelExtractor extractor = new PixelExtractor(new TestHeader(), new Product("A", "B", 2, 2), 1, null, null, true, new AffineTransform());
        extractor.extract(null);
    }

    @Test
    public void testFlipIntArray() throws Exception {
        int[] original = {
                0, 1,
                2, 3,
                4, 5
        };
        int[] flipY = {
                1, 0,
                3, 2,
                5, 4
        };
        int[] flipX = {
                4, 5,
                2, 3,
                0, 1
        };
        int[] flipBoth = {
                5, 4,
                3, 2,
                1, 0
        };

        assertArrayEquals(original, PixelExtractor.flipIntArray(new int[]{0, 1, 2, 3, 4, 5}, 2, 3, false, false));
        assertArrayEquals(flipY, PixelExtractor.flipIntArray(new int[]{0, 1, 2, 3, 4, 5}, 2, 3, false, true));
        assertArrayEquals(flipX, PixelExtractor.flipIntArray(new int[]{0, 1, 2, 3, 4, 5}, 2, 3, true, false));
        assertArrayEquals(flipBoth, PixelExtractor.flipIntArray(new int[]{0, 1, 2, 3, 4, 5}, 2, 3, true, true));

        assertArrayEquals(new int[]{1, 0}, PixelExtractor.flipIntArray(new int[]{0, 1}, 1, 2, true, true));
        assertArrayEquals(new int[]{3, 2, 1, 0}, PixelExtractor.flipIntArray(new int[]{0, 1, 2, 3}, 2, 2, true, true));
    }
}
