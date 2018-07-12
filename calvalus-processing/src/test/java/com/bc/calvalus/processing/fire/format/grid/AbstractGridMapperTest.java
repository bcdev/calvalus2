package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.processing.fire.format.grid.modis.ModisFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.modis.ModisGridMapper;
import com.bc.calvalus.processing.fire.format.grid.s2.S2FireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.s2.S2GridMapper;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author thomas
 */
public class AbstractGridMapperTest {

    private Logger logger;

    @Before
    public void setUp() throws Exception {
        logger = Logger.getAnonymousLogger();
        logger.setUseParentHandlers(false);

        TimestampFormatter formatter = new TimestampFormatter();
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(formatter);

        logger.addHandler(handler);

    }

    @Test
    public void testIsValidPixel() throws Exception {
        AbstractGridMapper gridMapper = new AbstractGridMapper(-1, -1) {
            @Override
            protected float getErrorPerPixel(double[] probabilityOfBurn, double area, int numberOfBurnedPixels) {
                return 0;
            }

            @Override
            protected void predict(float[] ba, double[] areas, float[] originalErrors) {

            }

            @Override
            protected void validate(float burnableFraction, List<float[]> baInLc, int targetGridCellIndex, double area) {

            }

            @Override
            protected boolean maskUnmappablePixels() {
                return false;
            }
        };

        assertTrue(gridMapper.isValidPixel(1, 31, 1));
        assertTrue(gridMapper.isValidPixel(1, 31, 7));
        assertTrue(gridMapper.isValidPixel(1, 31, 10));
        assertTrue(gridMapper.isValidPixel(1, 31, 14));
        assertTrue(gridMapper.isValidPixel(1, 31, 15));
        assertFalse(gridMapper.isValidPixel(32, 60, 16));
        assertFalse(gridMapper.isValidPixel(32, 60, 22));
        assertFalse(gridMapper.isValidPixel(32, 60, 31));
    }

    @Test
    public void testGetFraction() throws Exception {
        System.out.println(AbstractGridMapper.getFraction(1, 1000));
    }

    @Ignore
    @Test
    public void acceptanceTestS2GridFormat() throws Exception {
        List<Product> products = new ArrayList<>();
        Files.list(Paths.get("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting"))
                .filter(path -> path.getFileName().toString().startsWith("BA-T33PXM"))
                .forEach(path -> {
                    try {
                        products.add(ProductIO.readProduct(path.toFile()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        AbstractGridMapper mapper = new MyMapper();
        File file = new File("d:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\lc-2010-T33PXM.nc");
        Product lcProduct = ProductIO.readProduct(file);

        List<Product> lcProducts = new ArrayList<>();
        for (int i = 0; i < products.size(); i++) {
            lcProducts.add(lcProduct);
        }

        List<ZipFile> geoLookupTables = new ArrayList<>();
        Files.list(Paths.get("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting"))
                .filter(path -> path.getFileName().toString().startsWith("v")).forEach(path -> {
            try {
                geoLookupTables.add(new ZipFile(path.toFile()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        S2FireGridDataSource dataSource = new S2FireGridDataSource("v40h98", products.toArray(new Product[0]), lcProducts.toArray(new Product[0]), geoLookupTables);
        mapper.setDataSource(dataSource);

        GridCells gridCells = mapper.computeGridCells(2016, 1);
        Product product = new Product("test", "test", 8, 8);
        Band ba1 = product.addBand("ba1", ProductData.TYPE_FLOAT32);
        ba1.setData(new ProductData.Float(gridCells.ba));
        Band pa1 = product.addBand("pa1", ProductData.TYPE_FLOAT32);
        pa1.setData(new ProductData.Float(gridCells.patchNumber));
        Band co1 = product.addBand("co1", ProductData.TYPE_FLOAT32);
        co1.setData(new ProductData.Float(gridCells.coverage));
        Band er1 = product.addBand("er1", ProductData.TYPE_FLOAT32);
        er1.setData(new ProductData.Float(gridCells.errors));
        List<float[]> baInLcFirstHalf = gridCells.baInLc;
        for (int i = 0; i < baInLcFirstHalf.size(); i++) {
            float[] floats = baInLcFirstHalf.get(i);
            Band lcBand = product.addBand("lc" + i, ProductData.TYPE_FLOAT32);
            lcBand.setData(new ProductData.Float(floats));
        }

        Band buf = product.addBand("buf", ProductData.TYPE_FLOAT32);
        buf.setData(new ProductData.Float(gridCells.burnableFraction));

        product.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 8, 8, -14, 14, 0.25, 0.25, 0.0, 0.0));

        ProductIO.writeProduct(product, "c:\\ssd\\grid-format.nc", "NetCDF-CF");
    }

    private static class MyMapper extends S2GridMapper {

        MyMapper() {
            super();
        }

    }

    ;

    private static class TimestampFormatter extends Formatter {

        private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");

        public String format(LogRecord record) {
            StringBuilder builder = new StringBuilder(1000);
            builder.append(df.format(new Date(record.getMillis()))).append(" - ");
            builder.append("[").append(record.getSourceClassName()).append(".");
            builder.append(record.getSourceMethodName()).append("] - ");
            builder.append("[").append(record.getLevel()).append("] - ");
            builder.append(formatMessage(record));
            builder.append("\n");
            return builder.toString();
        }
    }

    @Ignore
    @Test
    public void acceptanceTestModisGridFormat() throws Exception {
        List<Product> products = new ArrayList<>();
        products.add(ProductIO.readProduct("C:\\ssd\\modis-analysis\\burned_2006_3_h10v02.nc"));

        AbstractGridMapper mapper = new ModisGridMapper();
        File lcFile = new File("C:\\ssd\\modis-analysis\\h10v02-2000.nc");
        Product lcProduct = ProductIO.readProduct(lcFile);

        Product[] products1 = products.toArray(new Product[0]);
        Product[] lcProducts = {lcProduct};
        ArrayList<ZipFile> geoLookupTables = new ArrayList<>();
        ZipFile geoLookupTable = new ZipFile("C:\\ssd\\modis-analysis\\geoluts\\modis-geo-luts-000x.zip");
        geoLookupTables.add(geoLookupTable);
        String targetTile = "3,92";
        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(products1, lcProducts, geoLookupTables, targetTile);
        mapper.setDataSource(dataSource);

        GridCells gridCells = mapper.computeGridCells(2006, 3);
        Product product = new Product("test", "test", 1, 1);
        Band ba1 = product.addBand("ba1", ProductData.TYPE_FLOAT32);
        ba1.setData(new ProductData.Float(gridCells.ba));
        Band pa1 = product.addBand("pa1", ProductData.TYPE_FLOAT32);
        pa1.setData(new ProductData.Float(gridCells.patchNumber));
        Band co1 = product.addBand("co1", ProductData.TYPE_FLOAT32);
        co1.setData(new ProductData.Float(gridCells.coverage));
        Band er1 = product.addBand("er1", ProductData.TYPE_FLOAT32);
        er1.setData(new ProductData.Float(gridCells.errors));
        List<float[]> baInLcFirstHalf = gridCells.baInLc;
        for (int i = 0; i < baInLcFirstHalf.size(); i++) {
            float[] floats = baInLcFirstHalf.get(i);
            Band lcBand = product.addBand("lc" + i, ProductData.TYPE_FLOAT32);
            lcBand.setData(new ProductData.Float(floats));
        }

        Band buf = product.addBand("buf", ProductData.TYPE_FLOAT32);
        buf.setData(new ProductData.Float(gridCells.burnableFraction));

        product.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 8, 8, -14, 14, 0.25, 0.25, 0.0, 0.0));

        ProductIO.writeProduct(product, "c:\\ssd\\modis-grid-format.nc", "NetCDF-CF");
    }

}