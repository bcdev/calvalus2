package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author thomas
 */
public class GridMapperTest {

    @Ignore
    @Test
    public void acceptanceTestComputeGridCell() throws Exception {
        GridMapper mapper = new GridMapper();
        Product product = ProductIO.readProduct("D:\\workspace\\fire-cci\\temp\\BA_PIX_MER_v04h24_200806_v4.0.tif");
        Product lcProduct = ProductIO.readProduct("D:\\workspace\\fire-cci\\temp\\lc-2005-v04h24.nc");
        File[] srFiles = new File("D:\\workspace\\fire-cci\\temp").listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("CCI-Fire-MERIS-SDR-");
            }
        });
        GridCell gridCell = mapper.computeGridCell(2008, 1, product, lcProduct, Arrays.asList(srFiles), new ErrorPredictor());
        System.out.println(gridCell);
    }

    @Test
    public void testIsValidFirstHalfPixel() throws Exception {
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 1));
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 7));
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 10));
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 14));
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 15));
        assertFalse(GridMapper.isValidFirstHalfPixel(1, 22, 16));
        assertFalse(GridMapper.isValidFirstHalfPixel(1, 22, 22));
        assertFalse(GridMapper.isValidFirstHalfPixel(1, 22, 31));

        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 1));
        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 7));
        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 10));
        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 14));
        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 15));
        assertTrue(GridMapper.isValidSecondHalfPixel(31, 7, 16));
        assertTrue(GridMapper.isValidSecondHalfPixel(31, 7, 22));
        assertTrue(GridMapper.isValidSecondHalfPixel(31, 7, 25));
        assertTrue(GridMapper.isValidSecondHalfPixel(31, 7, 31));

        assertTrue(GridMapper.isValidSecondHalfPixel(28, 7, 28));
    }
}