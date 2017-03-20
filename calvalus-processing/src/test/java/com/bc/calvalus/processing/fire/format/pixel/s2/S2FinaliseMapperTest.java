package com.bc.calvalus.processing.fire.format.pixel.s2;

import com.bc.calvalus.processing.fire.format.PixelProductArea;
import com.bc.calvalus.processing.fire.format.S2Strategy;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.Rectangle;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.pixel.s2.S2FinaliseMapper.TILE_SIZE;
import static org.junit.Assert.assertEquals;

public class S2FinaliseMapperTest {

    @Ignore
    @Test
    public void testRemap() throws Exception {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        Product lcProduct = ProductIO.readProduct("c:\\ssd\\2010.nc");
        lcProduct.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        String baseFilename = S2FinaliseMapper.createBaseFilename("2016", "02", "fv4.2", new S2Strategy().getArea("AREA_24"));
        Product product = S2FinaliseMapper.remap(new File("C:\\ssd\\L3_2016-02-01_2016-02-29.nc"), baseFilename, lcProduct, () -> System.out.println("S2FinaliseMapperTest.progress"));

        ProductIO.writeProduct(product, "C:\\ssd\\" + baseFilename + "_test256.tif", BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
    }

    @Test
    public void testFindNeighbourValue_1() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[56] = 20; // right next to the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 55, destRect.width).neighbourValue;
        assertEquals(20, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_with_precedence_1() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[44] = 10; // upper left of the source value
        sourceJdArray[56] = 20; // right next to the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 55, destRect.width).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_with_precedence_2() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[45] = 10; // upper center of the source value
        sourceJdArray[46] = 20; // upper right of the source value
        sourceJdArray[54] = 30; // center left of the source value
        sourceJdArray[56] = 40; // center right of the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 55, destRect.width).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[1] = 10; // center right of the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 0, destRect.width).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge2() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[88] = 10; // upper left of the source value
        sourceJdArray[89] = 20; // upper center of the source value
        sourceJdArray[98] = 30; // center left of the source value
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 99, destRect.width).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_exc() throws Exception {
        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[55] = 23;
        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) S2FinaliseMapper.findNeighbourValue(sourceJdArray, 55, destRect.width).neighbourValue;
        assertEquals(23, neighbourValue);
    }

    @Test
    public void testCreateMetadata() throws Exception {
        for (PixelProductArea area : new S2Strategy().getAllAreas()) {
            for (MonthYear monthYear : monthYears()) {
                String baseFilename = S2FinaliseMapper.createBaseFilename(monthYear.year, monthYear.month, S2FinaliseMapper.VERSION, area);
                String metadata = S2FinaliseMapper.createMetadata(monthYear.year, monthYear.month, S2FinaliseMapper.VERSION, area);
                try (FileWriter fw = new FileWriter("d:\\workspace\\fire-cci\\temp\\s2-pixel-metadata\\" + baseFilename + ".xml")) {
                    fw.write(metadata);
                }
            }
        }
    }

    private static MonthYear[] monthYears() {
        ArrayList<MonthYear> monthYears = new ArrayList<>();
        for (String m : new String[]{"06", "07", "08", "09", "10", "11", "12"}) {
            monthYears.add(new MonthYear(m, "2015"));
        }
        for (String m : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09"}) {
            monthYears.add(new MonthYear(m, "2016"));
        }
        return monthYears.toArray(new MonthYear[0]);
    }

    private static class MonthYear {
        String month;
        String year;

        public MonthYear(String month, String year) {
            this.month = month;
            this.year = year;
        }
    }


}