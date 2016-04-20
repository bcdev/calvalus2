package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Before;
import org.junit.Test;

import java.awt.Rectangle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

/**
 *
 * @author thomas
 */
public class FireGridDataSourceImplTest {

    private FireGridMapper.FireGridDataSource dataSource;

    @Before
    public void setUp() throws Exception {
        Map<FireGridMapper.Position, Product> neighbourProducts = new HashMap<>();
        /*

            TL-1  TC-4  TR-7
            CL-2  CC-5  CR-8
            BL-3  BC-6  CB-9

         */
        neighbourProducts.put(FireGridMapper.Position.TOP_LEFT, createProduct(1));
        neighbourProducts.put(FireGridMapper.Position.CENTER_LEFT, createProduct(2));
        neighbourProducts.put(FireGridMapper.Position.BOTTOM_LEFT, createProduct(3));
        neighbourProducts.put(FireGridMapper.Position.TOP_CENTER, createProduct(4));
        neighbourProducts.put(FireGridMapper.Position.BOTTOM_CENTER, createProduct(6));
        neighbourProducts.put(FireGridMapper.Position.TOP_RIGHT, createProduct(7));
        neighbourProducts.put(FireGridMapper.Position.CENTER_RIGHT, createProduct(8));
        neighbourProducts.put(FireGridMapper.Position.BOTTOM_RIGHT, createProduct(9));

        Product centerProduct = createProduct(5);
        dataSource = new FireGridDataSourceImpl(centerProduct, neighbourProducts);
    }

    @Test
    public void testReadPixels() throws Exception {
        int[] pixels = new int[4];

        // center
        dataSource.readPixels(new Rectangle(0, 0, 2, 2), pixels);
        int[] expected = {
                5000, 5001, 5004, 5005,
        };
        assertArrayEquals(expected, pixels);

        // top-left
        dataSource.readPixels(new Rectangle(-1, -1, 2, 2), pixels);
        expected = new int[] {
                1015, 2003, 4012, 5000
        };
        assertArrayEquals(expected, pixels);

        // center-left
        dataSource.readPixels(new Rectangle(-1, 0, 2, 2), pixels);
        expected = new int[] {
                2003, 2007, 5000, 5004
        };
        assertArrayEquals(expected, pixels);

        // bottom-left
        dataSource.readPixels(new Rectangle(-1, 3, 2, 2), pixels);
        expected = new int[] {
                2015, 3003, 5012, 6000
        };
        assertArrayEquals(expected, pixels);

        pixels = new int[9];

        // center larger
        dataSource.readPixels(new Rectangle(0, 0, 3, 3), pixels);
        expected = new int[] {
                5000, 5001, 5002, 5004, 5005, 5006, 5008, 5009, 5010
        };
        assertArrayEquals(expected, pixels);

        // top-left larger
        dataSource.readPixels(new Rectangle(-1, -1, 3, 3), pixels);
        expected = new int[] {
                1015, 2003, 2007, 4012, 4013, 5000, 5001, 5004, 5005
        };
        assertArrayEquals(expected, pixels);

        // center-left larger
        dataSource.readPixels(new Rectangle(-1, 0, 3, 3), pixels);
        expected = new int[] {
                2003, 2007, 2011, 5000, 5001, 5004, 5005, 5008, 5009
        };
        assertArrayEquals(expected, pixels);

        // bottom-left larger
        dataSource.readPixels(new Rectangle(-1, 3, 3, 3), pixels);
        expected = new int[] {
                2015, 3003, 3007, 5012, 5013, 6000, 6001, 6004, 6005
        };
        assertArrayEquals(expected, pixels);
    }

    private static Product createProduct(int value) {
        Product product = new Product("name", "type", 4, 4);
        Band band = product.addBand("band_1", ProductData.TYPE_INT32);
        int[] pixels = new int[4 * 4];
        Arrays.fill(pixels, value * 1000);
        for (int i = 0; i < pixels.length; i++) {
            pixels[i] = pixels[i] + i;
        }
        band.setRasterData(new ProductData.Int(pixels));
        return product;
    }


}