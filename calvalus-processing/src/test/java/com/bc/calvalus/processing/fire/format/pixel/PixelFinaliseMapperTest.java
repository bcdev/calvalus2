package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.processing.fire.format.pixel.GlobalPixelProductAreaProvider.GlobalPixelProductArea;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.TILE_SIZE;
import static org.junit.Assert.assertEquals;

public class PixelFinaliseMapperTest {

    @Ignore
    @Test
    public void testRemap() throws Exception {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        Product lcProduct = ProductIO.readProduct("c:\\ssd\\modis-analysis\\africa-2000.nc");
        lcProduct.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        Product product = PixelFinaliseMapper.remap(new File("C:\\ssd\\modis-analysis\\subset_0_of_L3_2006-03-01_2006-03-31.nc"), "c:\\ssd\\test.nc", "4", lcProduct, () -> System.out.println("PixelFinaliseMapperTest.progress"));

        ProductIO.writeProduct(product, "C:\\ssd\\test4.nc", "NetCDF4-CF");
    }

    @Test
    public void testFindNeighbourValue_1() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[56] = 20; // right next to the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        PixelFinaliseMapper.NeighbourResult neighbourResult = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true);

        int neighbourValue = (int) neighbourResult.neighbourValue;
        int newPixelIndex = neighbourResult.newPixelIndex;
        assertEquals(20, neighbourValue);
        assertEquals(56, newPixelIndex);
    }

    @Test
    public void testFindNeighbourValue_with_precedence_1() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[44] = 10; // upper left of the source value
        sourceJdArray[56] = 20; // right next to the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_with_precedence_2() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[45] = 10; // upper center of the source value
        sourceJdArray[46] = 20; // upper right of the source value
        sourceJdArray[54] = 30; // center left of the source value
        sourceJdArray[56] = 40; // center right of the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge() throws Exception {
        int pixelIndex = 0;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[1] = 10; // center right of the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge_2() throws Exception {
        int pixelIndex = 53248;
        int wrongNeighbourPixelIndex = 52991;

        float[] sourceJdArray = new float[256 * 256];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[wrongNeighbourPixelIndex] = 10;

        int[] lcArray = new int[256 * 256];
        Arrays.fill(lcArray, 210); // all non-burnable
        lcArray[wrongNeighbourPixelIndex] = 180; // burnable


        final Rectangle destRect = new Rectangle(19712, 25344, 256, 256);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(-2, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_on_edge2() throws Exception {
        int pixelIndex = 99;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[88] = 10; // upper left of the source value
        sourceJdArray[89] = 20; // upper center of the source value
        sourceJdArray[98] = 30; // center left of the source value

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(10, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_exc() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[pixelIndex] = 23;

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(23, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_non_burnable() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[pixelIndex + 1] = 23;
        sourceJdArray[pixelIndex + 10] = 24; // below start pixel

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        lcArray[pixelIndex + 1] = 25; // not burnable
        lcArray[pixelIndex + 10] = 180; // burnable


        final Rectangle destRect = new Rectangle(10, 10);
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true).neighbourValue;
        assertEquals(24, neighbourValue);
    }

    @Test
    public void testFindNeighbourValue_all_non_burnable() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[pixelIndex + 1] = 23;
        sourceJdArray[pixelIndex + 10] = 24; // below start pixel

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        lcArray[pixelIndex + 1] = 25; // not burnable
        lcArray[pixelIndex + 10] = 25; // not burnable


        final Rectangle destRect = new Rectangle(10, 10);
        PixelFinaliseMapper.NeighbourResult neighbourResult = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true);

        int neighbourValue = (int) neighbourResult.neighbourValue;
        int newPixelIndex = neighbourResult.newPixelIndex;
        assertEquals(-2, neighbourValue);
        assertEquals(pixelIndex, newPixelIndex);
    }

    @Test
    public void testFindNeighbourValue_all_non_burnable_cl() throws Exception {
        int pixelIndex = 55;

        float[] sourceJdArray = new float[100];
        Arrays.fill(sourceJdArray, Float.NaN);
        sourceJdArray[pixelIndex + 1] = 23;
        sourceJdArray[pixelIndex + 10] = 24; // below start pixel

        int[] lcArray = new int[100];
        Arrays.fill(lcArray, 180); // all burnable

        lcArray[pixelIndex + 1] = 25; // not burnable
        lcArray[pixelIndex + 10] = 25; // not burnable


        final Rectangle destRect = new Rectangle(10, 10);
        PixelFinaliseMapper.NeighbourResult neighbourResult = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, false);

        int neighbourValue = (int) neighbourResult.neighbourValue;
        int newPixelIndex = neighbourResult.newPixelIndex;
        assertEquals(0, neighbourValue);
        assertEquals(pixelIndex, newPixelIndex);
    }


    @Ignore
    @Test
    public void testCreateMetadata() throws Exception {
//        for (PixelProductArea area : new S2Strategy().getAllAreas()) {
        for (GlobalPixelProductArea area : GlobalPixelProductArea.values()) {
//            for (MonthYear monthYear : s2MonthYears()) {
            // "h37v17;185;90;190;95"
            String areaString = area.index + ";" + area.nicename + ";" + area.left + ";" + area.right + ";" + area.top + ";" + area.bottom;
            String baseFilename = PixelFinaliseMapper.createBaseFilename("2006", "03", "v5.0", areaString);
            String metadata = PixelFinaliseMapper.createMetadata("2006", "03", "v5.0", areaString);
            try (FileWriter fw = new FileWriter("c:\\ssd\\" + baseFilename + ".xml")) {
                fw.write(metadata);
            }
        }
//        }
    }


    private static MonthYear[] s2MonthYears() {
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