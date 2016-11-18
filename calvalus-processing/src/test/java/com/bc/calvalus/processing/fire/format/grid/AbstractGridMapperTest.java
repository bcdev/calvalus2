package com.bc.calvalus.processing.fire.format.grid;

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
    public void testIsValidFirstHalfPixel() throws Exception {
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 1));
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 7));
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 10));
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 14));
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 15));
        assertFalse(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 16));
        assertFalse(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 22));
        assertFalse(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 31));

        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 1));
        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 7));
        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 10));
        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 14));
        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 15));
        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 16));
        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 22));
        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 25));
        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 31));

        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(28, 7, 28));
    }

    @Ignore
    @Test
    public void acceptanceTestS2GridFormat() throws Exception {
        ErrorPredictor errorPredictor = new ErrorPredictor();
        List<Product> products = new ArrayList<>();
        products.add(ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\BA-T28PFU-20160101T113935.nc"));
        products.add(ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\BA-T28PFU-20160108T113011.nc"));
        products.add(ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\BA-T28PFU-20160111T113934.nc"));
        products.add(ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\BA-T28PFU-20160118T113008.nc"));
        products.add(ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\BA-T28PGA-20160108T113011.nc"));
        products.add(ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\BA-T28PGA-20160118T113008.nc"));
        products.add(ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\BA-T28PGU-20160108T113011.nc"));
        products.add(ProductIO.readProduct("D:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\BA-T28PGU-20160118T113008.nc"));

        AbstractGridMapper mapper = new AbstractGridMapper(8, 8) {
            @Override
            protected boolean maskUnmappablePixels() {
                return false;
            }
        };
        File file = new File("d:\\workspace\\fire-cci\\testdata\\for-grid-formatting\\lc-2010-v07h16.nc");
        Product lcProduct = ProductIO.readProduct(file);
        S2GridMapper.setGcToLcProduct(lcProduct);
        ZipFile geoLookupTable = new ZipFile("C:\\ssd\\v38h83-28PFU.zip");
        ZipFile geoLookupTable2 = new ZipFile("C:\\ssd\\v38h83-28PGA.zip");
        ZipFile geoLookupTable3 = new ZipFile("C:\\ssd\\v38h83-28PGU.zip");
        List<ZipFile> geoLookupTables = new ArrayList<>();
        geoLookupTables.add(geoLookupTable);
        geoLookupTables.add(geoLookupTable2);
        geoLookupTables.add(geoLookupTable3);

        S2FireGridDataSource dataSource = new S2FireGridDataSource("v38h83", products.toArray(new Product[0]), lcProduct, geoLookupTables, logger);
        mapper.setDataSource(dataSource);

        GridCell gridCell = mapper.computeGridCell(2016, 1, errorPredictor);
        Product product = new Product("test", "test", 8, 8);
        Band ba1 = product.addBand("ba1", ProductData.TYPE_FLOAT32);
        ba1.setData(new ProductData.Float(gridCell.baFirstHalf));
        Band pa1 = product.addBand("pa1", ProductData.TYPE_FLOAT32);
        pa1.setData(new ProductData.Float(gridCell.patchNumberFirstHalf));
        Band co1 = product.addBand("co1", ProductData.TYPE_FLOAT32);
        co1.setData(new ProductData.Float(gridCell.coverageFirstHalf));
        Band er1 = product.addBand("er1", ProductData.TYPE_FLOAT32);
        er1.setData(new ProductData.Float(gridCell.errorsFirstHalf));
        Band ba2 = product.addBand("ba2", ProductData.TYPE_FLOAT32);
        ba2.setData(new ProductData.Float(gridCell.baSecondHalf));
        Band pa2 = product.addBand("pa2", ProductData.TYPE_FLOAT32);
        pa2.setData(new ProductData.Float(gridCell.patchNumberSecondHalf));
        Band co2 = product.addBand("co2", ProductData.TYPE_FLOAT32);
        co2.setData(new ProductData.Float(gridCell.coverageSecondHalf));
        Band er2 = product.addBand("er2", ProductData.TYPE_FLOAT32);
        er2.setData(new ProductData.Float(gridCell.errorsSecondHalf));
        List<float[]> baInLcFirstHalf = gridCell.baInLcFirstHalf;
        for (int i = 0; i < baInLcFirstHalf.size(); i++) {
            float[] floats = baInLcFirstHalf.get(i);
            Band lcBand = product.addBand("lc" + i, ProductData.TYPE_FLOAT32);
            lcBand.setData(new ProductData.Float(floats));
        }

        product.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 8, 8, -14, 14, 0.25, 0.25, 0.0, 0.0));

        ProductIO.writeProduct(product, "c:\\ssd\\grid-format.nc", "NetCDF-CF");
    }

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

}