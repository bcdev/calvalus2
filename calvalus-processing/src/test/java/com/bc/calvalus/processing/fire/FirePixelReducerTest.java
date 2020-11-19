package com.bc.calvalus.processing.fire;

import com.bc.calvalus.processing.hadoop.RasterStackWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.esa.snap.core.datamodel.Product;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import ucar.ma2.InvalidRangeException;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.ArrayList;

import static org.esa.snap.core.dataio.ProductIO.readProduct;
import static org.junit.Assert.assertEquals;

public class FirePixelReducerTest {

    private Product product;
    private Product uncertainty;

    @Before
    public void setUp() throws Exception {
        product = readProduct(getClass().getResource("OLCIMODIS2019_1_h19v10_Classification.tif").getFile());
        uncertainty = readProduct(getClass().getResource("OLCIMODIS2019_1_h19v10_Uncertainty.tif").getFile());
    }

    @Test
    @Ignore("Writes output to local working directory, so should not be run automatically.")
    public void testReducer() throws IOException, FactoryException, TransformException, InvalidRangeException {
        short[] classificationPixels = new short[200];
        byte[] uncertaintyPixels = new byte[200];

        FirePixelMapper.readData(product, new Rectangle(1193, 0, 200, 1), classificationPixels);
        FirePixelMapper.readData(uncertainty, new Rectangle(1193, 0, 200, 1), uncertaintyPixels);

        RasterStackWritable rasterStackWritable = new RasterStackWritable(200, 1, 2);
        rasterStackWritable.setBandType(0, RasterStackWritable.Type.SHORT);
        rasterStackWritable.setData(0, classificationPixels, RasterStackWritable.Type.SHORT);
        rasterStackWritable.setBandType(1, RasterStackWritable.Type.BYTE);
        rasterStackWritable.setData(1, uncertaintyPixels, RasterStackWritable.Type.BYTE);

        FirePixelReducer reducer = new FirePixelReducer();
        Configuration configuration = new Configuration();
        configuration.set("calvalus.input.dateRanges", "[2019-12-01:2019-12-31]");
        configuration.set("calvalus.aux.lcMapPath", "D:\\workspace\\c3s\\C3S-LC-L4-LCCS-Map-300m-P1Y-2018-v2.1.1.tif");
        configuration.set("calvalus.regionGeometry", "POLYGON ((-26 25, 53 25, 53 -40, -26 -40, -26 25))");
        configuration.set("calvalus.regionName", "AREA_Test");
        configuration.set("calvalus.l3.parameters", "<parameters> <planetaryGrid>org.esa.snap.binning.support.PlateCarreeGrid</planetaryGrid> <numRows>64800</numRows> </parameters>");
        configuration.set("calvalus.output.dir", "file://c/ssd/c3s");
        configuration.set("calvalus.output.version", "v1.0test");
        reducer.setupInternal(configuration);
        ArrayList<RasterStackWritable> data = new ArrayList<>();
        data.add(rasterStackWritable);
        reducer.reduce(new LongWritable(4665668400L + 1193), data, null);
        reducer.closeNcFile();
    }

    @Test
    public void testFirstLevelLc() {
        byte lc = (byte) 152;
        int lc1 = LcRemapping.remap(lc);
        if (lc1 < 0) lc1 += 256;
        byte lc2 = (byte) lc1;
        assertEquals(lc1, 150);
        assertEquals(lc2, 150 - 256);
        lc = (byte) 62;
        lc1 = LcRemapping.remap(lc);
        if (lc1 < 0) lc1 += 256;
        lc2 = (byte) lc1;
        assertEquals(lc1, 60);
        assertEquals(lc2, 60);
    }
}