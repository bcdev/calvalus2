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

        dataSource.readPixels(new Rectangle(0, 0, 2, 2), pixels);
        int[] expected = {
                5000, 5001,
                5004, 5005,
        };
        assertArrayEquals(expected, pixels);

        dataSource.readPixels(new Rectangle(-1, 0, 2, 2), pixels);
        expected = new int[] {
                1003, 5000,
                1007, 5004
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