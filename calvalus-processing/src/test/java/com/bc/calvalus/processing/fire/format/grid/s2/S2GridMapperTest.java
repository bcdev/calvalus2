package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertEquals;

public class S2GridMapperTest {

    @Test
    @Ignore
    public void acceptanceTest_1() throws IOException {
        /* prerequisites:
            burned area files for 2deg-tile x174y98 of tile T30NUN
            the LC file corresponding to that tile
            the geo-lookup file x174y98-30NUN.zip
        */
        int doyFirstOfMonth = Year.of(2016).atMonth(1).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(2016).atMonth(1).atDay(Year.of(2016).atMonth(1).lengthOfMonth()).getDayOfYear();
        List<Product>[] products = getProducts("30NUN");
        List<ZipFile> geoLookupTables = getGeoLookupTables("x174y98", "30NUN");

        S2FireGridDataSource dataSource = new S2FireGridDataSource("x174y98", products[0].toArray(new Product[0]), null, products[1].toArray(new Product[0]));

        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        S2GridMapper s2GridMapper = new S2GridMapper();
        s2GridMapper.setDataSource(dataSource);
        GridCells gridCells = s2GridMapper.computeGridCells(2016, 1);

        double[] burnedArea = gridCells.ba;
        float[] patchNumbers = gridCells.patchNumber;
        float[] errors = gridCells.errors;
        float[] coverage = gridCells.coverage;
        float[] burnableFraction = gridCells.burnableFraction;

        Product product = new Product("output", "fire", 8, 8);
        product.addBand("ba", ProductData.TYPE_FLOAT32).setDataElems(burnedArea);
        product.addBand("pn", ProductData.TYPE_FLOAT32).setDataElems(patchNumbers);
        product.addBand("er", ProductData.TYPE_FLOAT32).setDataElems(errors);
        product.addBand("cov", ProductData.TYPE_FLOAT32).setDataElems(coverage);
        product.addBand("bf", ProductData.TYPE_FLOAT32).setDataElems(burnableFraction);

        ProductIO.writeProduct(product, "c:\\ssd\\s2-analysis\\grid\\test.nc", "NetCDF4-CF");

//        SourceData data = dataSource.readPixels(7, 6);
//
//        double areas = 0.0;
//        int numberOfBurnedPixels = 0;
//
//        for (int i = 0; i < data.burnedPixels.length; i++) {
//            areas += data.areas[i];
//            int doy = data.burnedPixels[i];
//            if (AbstractGridMapper.isValidPixel(doyFirstOfMonth, doyLastOfMonth, doy)) {
//                numberOfBurnedPixels++;
//            }
//        }
//
//        areas = Arrays.stream(data.areas).filter(value -> value > 0).average().getAsDouble();
//
//        float errorPerPixel = new S2GridMapper().getErrorPerPixel(data.probabilityOfBurn, areas, numberOfBurnedPixels);
//        assertEquals(8454.585, errorPerPixel, 1E-5);
//        assertEquals(163, data.patchCount);
    }

    @Test
    @Ignore
    public void acceptanceTest_2() throws IOException {
        /* prerequisites:
            burned area files for 2deg-tile x204y88 of tile 35MLS
            the LC file corresponding to that tile
            the geo-lookup file x204y88-35MLS.zip
        */
        int doyFirstOfMonth = Year.of(2016).atMonth(1).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(2016).atMonth(1).atDay(Year.of(2016).atMonth(1).lengthOfMonth()).getDayOfYear();
        List<Product>[] products = getProducts("35MLS");
        List<ZipFile> geoLookupTables = getGeoLookupTables("x204y88", "35MLS");

        S2FireGridDataSource dataSource = new S2FireGridDataSource("x204y88", products[0].toArray(new Product[0]), null, products[1].toArray(new Product[0]));

        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);
        SourceData data = dataSource.readPixels(6, 5);

        double areas = 0.0;
        int numberOfBurnedPixels = 0;

        for (int i = 0; i < data.burnedPixels.length; i++) {
            areas += data.areas[i];
            float doy = data.burnedPixels[i];
            if (new S2GridMapper().isActuallyBurnedPixel(doyFirstOfMonth, doyLastOfMonth, doy, true)) {
                numberOfBurnedPixels++;
            }
        }

        areas = Arrays.stream(data.areas).filter(value -> value > 0).average().getAsDouble();

        float errorPerPixel = new S2GridMapper().getErrorPerPixel(data.probabilityOfBurn, areas, 0);
        assertEquals(11063.7255859375, errorPerPixel, 1E-5);
        assertEquals(40, data.patchCount);
    }

    protected static float getFraction(double value, double area) {
        float fraction = (float) (value / area) >= 1.0F ? 1.0F : (float) (value / area);
        if (Float.isNaN(fraction)) {
            fraction = 0.0F;
        }
        return fraction;
    }

    private static List<Product>[] getProducts(String tile) throws IOException {
        List<Product> products = new ArrayList<>(2);
        List<Product> lcProducts = new ArrayList<>(2);
        final int[] count = new int[]{0};
        Files.list(Paths.get("D:\\workspace\\fire-cci\\temp"))
//                .filter(path -> path.getFileName().toString().contains("NUN"))
                .filter(path -> path.getFileName().toString().startsWith("BA"))
                .filter(path -> path.getFileName().toString().contains(tile))
                .forEach(path -> {
                    try {
                        count[0]++;
                        System.out.println(count[0]);
                        Product product = ProductIO.readProduct(path.toFile());
                        Product lcProduct = ProductIO.readProduct("D:\\workspace\\fire-cci\\temp\\lc-2010-T" + tile + ".nc");
                        products.add(product);
                        lcProducts.add(lcProduct);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
        List<Product>[] lists = new List[2];
        lists[0] = products;
        lists[1] = lcProducts;
        return lists;
    }

    private static List<Product> getLcProducts() throws IOException {
        List<Product> products = new ArrayList<>();
        Files.list(Paths.get("D:\\workspace\\fire-cci\\temp"))
                .filter(path -> path.getFileName().toString().contains("lc-"))
                .forEach(path -> {
                    try {
                        Product product = ProductIO.readProduct(path.toFile());
                        products.add(product);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
        return products;
    }

    private static List<ZipFile> getGeoLookupTables(String twoDegTile, String s2Tile) throws IOException {
        List<ZipFile> geoLookupTables = new ArrayList<>();
        Files.list(Paths.get("D:\\workspace\\fire-cci\\temp"))
                .filter(path -> path.getFileName().toString().endsWith(".zip"))
                .filter(path -> path.getFileName().toString().contains(twoDegTile))
                .filter(path -> path.getFileName().toString().contains(s2Tile))
                .forEach(path -> {
                    try {
                        geoLookupTables.add(new ZipFile(path.toFile()));
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
        return geoLookupTables;
    }


    @Test
    public void getErrorPerPixel() throws Exception {
        double[] probs = {
                0.56536696, 0.78055542, 0.65436347, 0.49271366, 0.76923176,
                0.49461837, 0.9761726, 0.38753145, 0.66194844, 0.73673713,
                0.8715599, 0.59177441, 0.19985001, 0.28629937, 0.70381885,
                0.98793774, 0.69215503, 0.27554467, 0.86892036, 0.64182099,
                0.29181729, 0.36298165, 0.7607316, 0.17223196, 0.86198424,
                0.1708366, 0.51868964, 0.02689661, 0.84591583, 0.76678544,
                0.39665975, 0.80400718, 0.06069604, 0.48492104, 0.13077186,
                0.36312609, 0.08083608, 0.22875986, 0.34459695, 0.32392777,
                0.25347801, 0.79042851, 0.60896279, 0.97076222, 0.64707434,
                0.62816483, 0.9933599, 0.80350037, 0.60894003, 0.7523323,
                0.26354999, 0.65738439, 0.12251649, 0.87043311, 0.15280995,
                0.20102399, 0.4567852, 0.19667328, 0.54721173, 0.76162707,
                0.47906019, 0.0786606, 0.33313626, 0.96322023, 0.61135772,
                0.11957433, 0.61950483, 0.37935909, 0.48772916, 0.29030689,
                0.88138267, 0.73888071, 0.20758538, 0.19298296, 0.9168182,
                0.58207931, 0.75525455, 0.9359131, 0.86915467, 0.1297407,
                0.45974684, 0.76526864, 0.56430141, 0.03041237, 0.65969498,
                0.62279958, 0.59339677, 0.9867703, 0.20068683, 0.88867341,
                0.0306582, 0.77244542, 0.80275072, 0.01832522, 0.30206282,
                0.93967375, 0.83246437, 0.06709965, 0.37869067, 0.1504346
        };
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(0.004992405883967876, new S2GridMapper().getErrorPerPixel(probs, 0.5, 0), 1E-6);

    }

    @Test
    public void testGetErrorPerPixelNoProbs() throws Exception {
        double[] probs = new double[1000];
        Arrays.fill(probs, 0);
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(0, new S2GridMapper().getErrorPerPixel(probs, 0.5, 0), 1E-6);
    }

    @Test
    public void testGetErrorPerPixelNaN() throws Exception {
        double[] probs = new double[5];
        probs[0] = 0;
        probs[1] = 0.5;
        probs[2] = 0.2;
        probs[3] = 0.534;
        probs[4] = 0.51;
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(0.0952223390340805, new S2GridMapper().getErrorPerPixel(probs, 0.5, 0), 1E-6);
        probs = new double[6];
        probs[0] = 0;
        probs[1] = 0.5;
        probs[2] = 0.2;
        probs[3] = 0.534;
        probs[4] = 0.51;
        probs[5] = Float.NaN;
        areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(0.07774871587753296, new S2GridMapper().getErrorPerPixel(probs, 0.5, 0), 1E-6);
    }

    @Test
    public void testGetErrorPerPixelZero() throws Exception {
        double[] probs = new double[5];
        probs[0] = 0;
        probs[1] = 0.5;
        probs[2] = 0.2;
        probs[3] = 0.534;
        probs[4] = 0.51;
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(0.0952223390340805, new S2GridMapper().getErrorPerPixel(probs, 0.5, 0), 1E-5);
    }

    @Test
    public void testGetErrorPerPixelInf() throws Exception {
        double[] probs = new double[1];
        probs[0] = 0.3;
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(1.0, new S2GridMapper().getErrorPerPixel(probs, 0.5, 0), 1E-6);
    }

    @Test
    public void testGetErrorPerPixelRealValues() throws Exception {
        double[] probBurn;
        try (GZIPInputStream zis = new GZIPInputStream(getClass().getResourceAsStream("prob-burn-s2.dat.gz"))) {
            ObjectInputStream iis = new ObjectInputStream(zis);
            probBurn = (double[]) iis.readObject();
        }

        double area = 7.671923035701257E8;
        int numBurned = 567875;

        float errorPerPixel = new S2GridMapper().getErrorPerPixel(probBurn, area, 0);
        System.out.println(String.format("%.12f", errorPerPixel));
        assertEquals(14483.97949F, errorPerPixel, 1E-5);
    }

    @Test
    public void name() throws IOException {
        Product product = ProductIO.readProduct("C:\\ssd\\2016.nc");
        for (int y = 0; y <= product.getSceneRasterHeight(); y += 5000) {
            for (int x = 0; x <= product.getSceneRasterWidth(); x += 5000) {
                System.out.println("Handling product " + " LC_" + x + "_" + y + ".nc...");

                SubsetOp subsetOp = new SubsetOp();
                subsetOp.setRegion(new Rectangle(x, y, Math.min(x, product.getSceneRasterWidth()), Math.min(y, product.getSceneRasterHeight())));
                subsetOp.setSourceProduct(product);
                ProductIO.writeProduct(subsetOp.getSourceProduct(), "D:\\workspace\\fire-cci\\s2-lc\\LC_" + x + "_" + y + ".nc", "NetCDF4-CF");
            }
        }
        System.out.print("...done");
    }
}