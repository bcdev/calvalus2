package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.Rectangle;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author thomas
 */
public class FireGridDataSourceImplTest {

    private FireGridDataSourceImpl dataSource;

    @Before
    public void setUp() throws Exception {
        Product centerProduct = createProduct(5);
        dataSource = new FireGridDataSourceImpl(centerProduct);
        dataSource.setDoyFirstHalf(7);
        dataSource.setDoySecondHalf(22);
        dataSource.setDoyFirstOfMonth(1);
        dataSource.setDoyLastOfMonth(31);
    }

    @Test
    public void testReadPixels() throws Exception {
        int[] pixels = new int[4];
        double[] areas = new double[4];

        SourceData data = new SourceData(pixels, areas);

        // center
        dataSource.readPixels(new Rectangle(0, 0, 2, 2), data);
        int[] expected = {
                5000, 5001, 5004, 5005,
        };
        assertArrayEquals(expected, pixels);
    }

    @Test
    public void testReadPixelsLarger() throws Exception {
        int[] pixels = new int[9];
        double[] areas = new double[9];

        SourceData data = new SourceData(pixels, areas);

        // center larger
        dataSource.readPixels(new Rectangle(0, 0, 3, 3), data);
        int[] expected = new int[]{
                5000, 5001, 5002, 5004, 5005, 5006, 5008, 5009, 5010
        };
        assertArrayEquals(expected, pixels);
    }

    @Test
    public void testGetPatches_0() throws Exception {
        int[] pixels = {
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0
        };
        assertEquals(0, dataSource.getPatchNumbers(FireGridDataSourceImpl.make2Dims(pixels), true));
    }

    @Test
    public void testGetPatches_1() throws Exception {
        int[] pixels = {
                0, 0, 0, 1,
                0, 0, 0, 1,
                1, 0, 0, 0,
                1, 0, 0, 1
        };
        assertEquals(3, dataSource.getPatchNumbers(FireGridDataSourceImpl.make2Dims(pixels), true));
    }

    @Test
    public void testGetPatches_2() throws Exception {
        int[] pixels = {
                0, 0, 0, 0,
                0, 1, 0, 0,
                0, 1, 1, 0,
                0, 0, 0, 0
        };
        assertEquals(1, dataSource.getPatchNumbers(FireGridDataSourceImpl.make2Dims(pixels), true));
    }

    @Test
    public void testGetPatches_3() throws Exception {
        int[] pixels = {
                1, 0, 1, 1,
                0, 1, 0, 1,
                0, 1, 1, 0,
                0, 0, 0, 1
        };
        assertEquals(4, dataSource.getPatchNumbers(FireGridDataSourceImpl.make2Dims(pixels), true));
    }

    @Test
    public void testGetPatches_Large() throws Exception {
        int[] pixels = new int[90 * 90];
        for (int i = 0; i < pixels.length; i++) {
            pixels[i] = (int) (Math.random() * 2);
        }
        dataSource.getPatchNumbers(FireGridDataSourceImpl.make2Dims(pixels), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMake2Dims_failing() throws Exception {
        FireGridDataSourceImpl.make2Dims(new int[]{
                1, 0, 1, 1,
                0, 1, 0, 1,
                0, 1, 1, 0
        });
    }

    @Test
    public void testMake2Dims() throws Exception {
        int[][] ints = FireGridDataSourceImpl.make2Dims(new int[]{
                1, 0, 1, 1,
                0, 1, 0, 1,
                0, 1, 1, 0,
                0, 0, 0, 1
        });

        assertArrayEquals(new int[]{1, 0, 1, 1}, ints[0]);
        assertArrayEquals(new int[]{0, 1, 0, 1}, ints[1]);
        assertArrayEquals(new int[]{0, 1, 1, 0}, ints[2]);
        assertArrayEquals(new int[]{0, 0, 0, 1}, ints[3]);
    }

    private static Product createProduct(int value) throws FactoryException, TransformException {
        Product product = new Product("name", "type", 4, 4);
        product.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 4, 4, 0, 0, 0.02, 0.02));
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