package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.processing.fire.format.PixelProductArea;
import com.bc.calvalus.processing.fire.format.S2Strategy;
import com.bc.calvalus.processing.fire.format.pixel.modis.ModisPixelFinaliseMapper;
import com.bc.calvalus.processing.fire.format.pixel.s2.S2PixelFinaliseMapper;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.image.ColoredBandImageMultiLevelSource;
import org.junit.Ignore;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.CL;
import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.JD;
import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.LC;
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

        Product lcProduct = ProductIO.readProduct("c:\\ssd\\modis-analysis\\pixel\\south_america-2005.nc");
        lcProduct.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        final File localL3 = new File("C:\\ssd\\modis-analysis\\pixel\\L3_2003-09-01_2003-09-30.nc");
        Product sourceProduct = ProductIO.readProduct(localL3);

        lcProduct = new ModisPixelFinaliseMapper().collocateWithSource(lcProduct, sourceProduct);

        writeSubset(lcProduct, sourceProduct, JD);
        writeSubset(lcProduct, sourceProduct, CL);
        writeSubset(lcProduct, sourceProduct, LC);
//        ProductIO.writeProduct(product, "C:\\ssd\\modis-analysis\\pixel\\test.nc", "NetCDF4-CF");
//        Product clproduct = new S2PixelFinaliseMapper().remap(sourceProduct, "CL", lcProduct, CL);
//        ProductIO.writeProduct(clproduct, "C:\\ssd\\modis-analysis\\pixel\\test-cl.nc", "NetCDF4-CF");
//        Product lcproduct = new S2PixelFinaliseMapper().remap(sourceProduct, "LC", lcProduct, LC);
//        ProductIO.writeProduct(lcproduct, "C:\\ssd\\modis-analysis\\pixel\\test-lc.nc", "NetCDF4-CF");
    }

    private static void writeSubset(Product lcProduct, Product sourceProduct, int band) throws IOException {
        Product product = new ModisPixelFinaliseMapper().remap(sourceProduct, "JD", lcProduct, band, "");
        SubsetOp subsetOp = new SubsetOp();
        subsetOp.setParameterDefaultValues();
        subsetOp.setRegion(new Rectangle(10000, 10000, 4000, 4000));
        subsetOp.setSourceProduct(product);

        ProductIO.writeProduct(subsetOp.getTargetProduct(), "C:\\ssd\\modis-analysis\\pixel\\test1" + band + ".nc", "NetCDF4-CF");
    }

    @Test
    public void testRemap2() throws Exception {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "" + TILE_SIZE);
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");

        Product lcProduct = ProductIO.readProduct("D:\\workspace\\temp\\collocate.dim");
        lcProduct.setPreferredTileSize(TILE_SIZE, TILE_SIZE);
        final File localL3 = new File("D:\\workspace\\temp\\subset_0_of_L3_2016-11-01_2016-11-30.dim");
        Product product = getPixelFinaliseMapper().remap(ProductIO.readProduct(localL3), "wumpel", lcProduct, 0, "");

        ProductIO.writeProduct(product, "C:\\ssd\\test6.nc", "NetCDF4-CF");
    }

    private static PixelFinaliseMapper getPixelFinaliseMapper() {
        return new PixelFinaliseMapper() {
            @Override
            protected ClScaler getClScaler() {
                return cl -> cl;
            }

            @Override
            public String createBaseFilename(String year, String month, String version, String areaString) {
                return "";
            }

            @Override
            protected Product collocateWithSource(Product lcProduct, Product source) {
                return lcProduct;
            }

            @Override
            protected String getCalvalusSensor() {
                return "S2";
            }

            @Override
            protected Band getLcBand(Product lcProduct) {
                return lcProduct.getBand("band_1");
            }
        };
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
        PixelFinaliseMapper.PositionAndValue positionAndValue = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2");

        int neighbourValue = (int) positionAndValue.value;
        int newPixelIndex = positionAndValue.newPixelIndex;
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
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2").value;
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
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2").value;
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
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2").value;
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
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2").value;
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
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2").value;
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
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2").value;
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
        int neighbourValue = (int) PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2").value;
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
        PixelFinaliseMapper.PositionAndValue positionAndValue = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, "S2");

        int neighbourValue = (int) positionAndValue.value;
        int newPixelIndex = positionAndValue.newPixelIndex;
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
        PixelFinaliseMapper.PositionAndValue positionAndValue = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, false, "S2");

        int neighbourValue = (int) positionAndValue.value;
        int newPixelIndex = positionAndValue.newPixelIndex;
        assertEquals(0, neighbourValue);
        assertEquals(pixelIndex, newPixelIndex);
    }


    @Ignore
    @Test
    public void testCreateMetadata() throws Exception {
        for (PixelProductArea area : new S2Strategy().getAllAreas()) {
//        for (GlobalPixelProductArea area : GlobalPixelProductArea.values()) {
            System.out.println(area.nicename);
//            for (MonthYear monthYear : s2MonthYears()) {
            // "h37v17;185;90;190;95"
            for (int year = 2016; year <= 2016; year++) {
                String targetDir = "c:\\ssd\\" + year;
                if (Files.notExists(Paths.get(targetDir))) {
                    Files.createDirectory(Paths.get(targetDir));
                }
                for (int month = 1; month <= 12; month++) {
                    String monthPad = month < 10 ? "0" + month : "" + month;
                    String areaString = area.index + ";" + area.nicename + ";" + area.left + ";" + area.top + ";" + area.right + ";" + area.bottom;
                    String baseFilename = new S2PixelFinaliseMapper().createBaseFilename(year + "", monthPad, "FireCCISFD1", areaString);
                    String metadata = PixelFinaliseMapper.createMetadata(PixelFinaliseMapper.S2_TEMPLATE, year + "", monthPad, "FireCCISFD1", areaString);
                    try (FileWriter fw = new FileWriter(targetDir + "\\" + baseFilename + ".xml")) {
                        fw.write(metadata);
                    }
                }
            }
            break;
        }
    }


    @Test
    public void testCreateModisMetadata() throws Exception {
        String targetDir = "c:\\ssd\\";
        PixelProductArea area = new GlobalPixelProductAreaProvider().getArea(GlobalPixelProductAreaProvider.GlobalPixelProductArea.SOUTH_AMERICA.name());
        String areaString = area.index + ";" + area.nicename + ";" + area.left + ";" + area.top + ";" + area.right + ";" + area.bottom;
        String baseFilename = new ModisPixelFinaliseMapper().createBaseFilename("2010" + "", "01", "FireCCI51", areaString);

        String metadata = PixelFinaliseMapper.createMetadata(PixelFinaliseMapper.MODIS_TEMPLATE, "2010", "01", "fv5.1", areaString);
        try (FileWriter fw = new FileWriter(targetDir + "\\" + baseFilename + ".xml")) {
            fw.write(metadata);
        }
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


    @Ignore
    @Test
    public void makeTimeSeriesOfRGBs() throws Exception {
        String basePath = "D:\\workspace\\fire-cci\\timeseries-4-emilio";
        Files.list(Paths.get(basePath))
                .filter(path -> path.toString().contains(".tif"))
                .forEach(path -> {
                    try {
                        Product product = ProductIO.readProduct(path.toFile());
                        File targetFile = new File(basePath + "\\" + product.getName() + ".png");
                        if (targetFile.exists()) {
                            return;
                        }
                        System.out.println("Creating RGB at " + targetFile.getName());

                        Band b12 = product.getBand("B12");
                        Band b11 = product.getBand("B11");
                        Band b4 = product.getBand("B4");
                        Band[] rasters = new Band[]{
                                b12, b11, b4
                        };
                        ColoredBandImageMultiLevelSource source = ColoredBandImageMultiLevelSource.create(rasters, ProgressMonitor.NULL);
                        RenderedImage image1 = source.createImage(3);

                        ImageIO.write(image1, "PNG", targetFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }


    @Test
    public void name() throws Exception {
        System.out.println(new Polygon(new int[]{11170, 11170, 8942, 9888, 10141, 10087, 10277, 11147}, new int[]{20407, 27816, 27816, 25623, 24088, 23259, 21898, 20271}, 8)
        .contains(new Point(10705, 22652)));
    }
}