package com.bc.calvalus.processing.fire.format.grid.meris;

import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author thomas
 */
public class MerisDataSourceTest {

    private Product lcProduct;
    private Product centerProduct;

    @Before
    public void setUp() throws Exception {
        centerProduct = createProduct(5);
        lcProduct = createProduct(10);
    }

    @Test
    public void testReadPixels() throws Exception {
        MerisDataSource dataSource = new MerisDataSource(centerProduct, lcProduct, new ArrayList<>(), 2, 2);
        dataSource.setDoyFirstHalf(7);
        dataSource.setDoySecondHalf(22);
        dataSource.setDoyFirstOfMonth(1);
        dataSource.setDoyLastOfMonth(31);

        // center
        SourceData data = dataSource.readPixels(0, 0);
        int[] expected = {
                5000, 5001, 5004, 5005,
        };
        assertArrayEquals(expected, data.pixels);
    }

    @Test
    public void testReadPixelsLarger() throws Exception {
        MerisDataSource dataSource = new MerisDataSource(centerProduct, lcProduct, new ArrayList<>(), 3, 3);
        dataSource.setDoyFirstHalf(7);
        dataSource.setDoySecondHalf(22);
        dataSource.setDoyFirstOfMonth(1);
        dataSource.setDoyLastOfMonth(31);

        // center larger
        int[] pixels = dataSource.readPixels(0, 0).pixels;
        int[] expected = new int[]{
                5000, 5001, 5002, 5004, 5005, 5006, 5008, 5009, 5010
        };
        assertArrayEquals(expected, pixels);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMake2Dims_failing() throws Exception {
        GridFormatUtils.make2Dims(new int[]{
                1, 0, 1, 1,
                0, 1, 0, 1,
                0, 1, 1, 0
        });
    }

    @Test
    public void testMake2Dims() throws Exception {
        int[][] ints = GridFormatUtils.make2Dims(new int[]{
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

    @Test
    public void testCollectStatusPixels() throws Exception {
        int[] target = {0, 1, 0, 0, 0};
        MerisDataSource.collectStatusPixels(new byte[]{1, 0, 0, 1, 0}, target);
        assertArrayEquals(new int[]{1, 1, 0, 1, 0}, target);
    }

    private static Product createProduct(int value) throws FactoryException, TransformException {
        Product product = new Product("name", "type", 4, 4);
        product.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 4, 4, 0, 0, 0.02, 0.02));
        Band band = product.addBand("band_1", ProductData.TYPE_INT32);
        Band band2 = product.addBand("lcclass", ProductData.TYPE_INT32);
        int[] pixels = new int[4 * 4];
        Arrays.fill(pixels, value * 1000);
        for (int i = 0; i < pixels.length; i++) {
            pixels[i] = pixels[i] + i;
        }
        band.setRasterData(new ProductData.Int(pixels));
        band2.setRasterData(new ProductData.Int(pixels));
        return product;
    }


}