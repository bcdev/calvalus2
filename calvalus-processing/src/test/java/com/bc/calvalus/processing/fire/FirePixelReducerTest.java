package com.bc.calvalus.processing.fire;

import com.bc.calvalus.processing.hadoop.RasterStackWritable;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Before;
import org.junit.Test;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.ArrayList;

public class FirePixelReducerTest {

    private Product product;
    private Product uncertainty;

    @Before
    public void setUp() throws Exception {
        product = ProductIO.readProduct(getClass().getResource("OLCIMODIS2019_1_h19v10_Classification.tif").getFile());
        uncertainty = ProductIO.readProduct(getClass().getResource("OLCIMODIS2019_1_h19v10_Uncertainty.tif").getFile());
    }

    @Test
    public void testReducer() throws IOException, InterruptedException {
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
        configuration.set("calvalus.year", "2019");
        configuration.set("calvalus.month", "12");
        configuration.set("calvalus.regionGeometry", "POLYGON ((-26 25, 53 25, 53 -40, -26 -40, -26 25))");
        configuration.set("calvalus.output.dir", "file://c/ssd/c3s");
        reducer.init(configuration);
        ArrayList<RasterStackWritable> list = new ArrayList<>();
        list.add(rasterStackWritable);
//        reducer.reduce(new LongWritable(4665668400L + 1193), list, null);
    }

}