package com.bc.calvalus.processing.fire.format.grid.olci;

import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class OlciDataSourceTest {

    private OlciDataSource olciDataSource;

        /*

        1 grid cell: 0.25°
            --> 4 grid cells per degree

        10 degree per input BA image
        3600 pixels per input BA image
            --> 3600 / 10 / 4 = 90 BA pixels per grid cell, width and height

        1 data source = 40 * 40 grid cells in OLCI case (3600x3600 / 90x90)
         */

        /*
        Tests here:
            read 90x90 pixels from x 1800 y 1620 (x 20, y 18)
         */

    @Before
    public void setUp() throws Exception {
        File classification = new File(getClass().getResource("OLCIMODIS2018_1_h31v10_Classification.tif").toURI());
        File foa = new File(getClass().getResource("OLCIMODIS2018_1_h31v10_FractionOfObservedArea.tif").toURI());
        File uncertainty = new File(getClass().getResource("OLCIMODIS2018_1_h31v10_Uncertainty.tif").toURI());
        File lc = new File(getClass().getResource("lc-h31v10.nc").toURI());

        Product classificationProduct = ProductIO.readProduct(classification);
        Product foaProduct = ProductIO.readProduct(foa);
        Product uncertaintyProduct = ProductIO.readProduct(uncertainty);
        Product lcProduct = ProductIO.readProduct(lc);
        olciDataSource = new OlciDataSource(classificationProduct, foaProduct, uncertaintyProduct, lcProduct);
    }

    @Test
    public void testReadPixelsStatus() throws IOException {
        SourceData sourceData = olciDataSource.readPixels(20, 18);
        for (int i = 0; i < 35 * 90 + 88; i++) {
            assertEquals(0, sourceData.statusPixels[i], 1E-5);
        }
        assertEquals(1, sourceData.statusPixels[35 * 90 + 88], 1E-5);
        assertEquals(1, sourceData.statusPixels[35 * 90 + 89], 1E-5);
        assertEquals(1, sourceData.statusPixels[36 * 90 + 88], 1E-5);
        assertEquals(1, sourceData.statusPixels[36 * 90 + 89], 1E-5);
        for (int i = 46 * 90; i < 76 * 90 + 59; i++) {
            assertEquals(0, sourceData.statusPixels[i], 1E-5);
        }
        assertEquals(1, sourceData.statusPixels[76 * 90 + 59], 1E-5);
        assertEquals(1, sourceData.statusPixels[76 * 90 + 60], 1E-5);
    }

    @Test
    public void testReadPixelsUncertainty() throws IOException {
        SourceData sourceData = olciDataSource.readPixels(20, 18);
        for (int i = 0; i < 90 * 90; i++) {
            assertEquals(0.0, sourceData.probabilityOfBurn[i], 1E-5);
        }
    }

    @Test
    public void testReadPixelsBA() throws IOException {
        SourceData sourceData = olciDataSource.readPixels(20, 18);
        for (int i = 0; i < 35 * 90 + 88; i++) {
            assertEquals(-1, sourceData.burnedPixels[i], 1E-5);
        }
        assertEquals(-2, sourceData.burnedPixels[35 * 90 + 88], 1E-5);
        assertEquals(-2, sourceData.burnedPixels[35 * 90 + 89], 1E-5);
        assertEquals(-2, sourceData.burnedPixels[36 * 90 + 88], 1E-5);
        assertEquals(-2, sourceData.burnedPixels[36 * 90 + 89], 1E-5);

        for (int i = 46 * 90; i < 76 * 90 + 59; i++) {
            assertEquals(-1, sourceData.burnedPixels[i], 1E-5);
        }
        assertEquals(-2, sourceData.burnedPixels[76 * 90 + 59], 1E-5);
        assertEquals(-2, sourceData.burnedPixels[76 * 90 + 60], 1E-5);
    }

    @Test
    public void testReadPixelsLc() throws IOException {
        SourceData sourceData = olciDataSource.readPixels(20, 18);
        for (int i = 0; i <= 20; i++) {
            assertEquals(122, sourceData.lcClasses[i]);
        }
        assertEquals(120, sourceData.lcClasses[21]);
        assertEquals(122, sourceData.lcClasses[22]);
        assertEquals(100, sourceData.lcClasses[23]);
        assertEquals(62, sourceData.lcClasses[73 * 90 + 42]);
        assertEquals(62, sourceData.lcClasses[74 * 90 + 41]);
        assertEquals(122, sourceData.lcClasses[74 * 90 + 42]);

        for (int i = 84; i <= 89; i++) {
            assertEquals(122, sourceData.lcClasses[89 * 90 + i]);
        }

    }

    @Ignore
    @Test
    public void splitLcData() throws IOException {
        Product lcProduct = ProductIO.readProduct("c:\\ssd\\c3s\\C3S-LC-L4-LCCS-Map-300m-P1Y-2017-v2.1.1.nc");
        for (String olciTile : OlciGridInputFormat.OLCI_TILES) {
            System.out.println(olciTile);
            int h = new Integer(olciTile.substring(1, 3));
            int v = new Integer(olciTile.substring(4, 6));
            int x0 = h * 3600;
            int y0 = v * 3600;
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("bandNames", new String[]{"lccs_class"});
            parameters.put("region", new Rectangle(x0, y0, 3600, 3600));
            Product subset = GPF.createProduct("Subset", parameters, lcProduct);
            ProductIO.writeProduct(subset, "d:\\workspace\\c3s\\splitted-lc\\lc-" + olciTile + ".nc", "NetCDF4-CF");
        }
    }
}