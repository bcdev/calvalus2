package org.esa.beam.binning;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.TiePointGeoCoding;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.esa.beam.framework.gpf.OperatorException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import static java.lang.Math.sqrt;
import static org.junit.Assert.*;

/**
 * @author Norman Fomferra
 */
public class BinningOpTest {
    int productCounter = 1;

    /**
     * The following configuration generates a 1-degree resolution global product (360 x 180 pixels) from 5 observations.
     * Values are only generated for pixels at x=180..181 and y=87..89.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testGlobalBinning() throws Exception {

        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final BinningOp binningOp = new BinningOp();

        binningOp.setSourceProducts(createSourceProduct(obs1),
                                    createSourceProduct(obs2),
                                    createSourceProduct(obs3),
                                    createSourceProduct(obs4),
                                    createSourceProduct(obs5));

        binningOp.setStartDate("2002-01-01");
        binningOp.setEndDate("2002-01-10");
        binningOp.setBinningConfig(binningConfig);
        binningOp.setFormatterConfig(formatterConfig);

        final Product targetProduct = binningOp.getTargetProduct();
        assertNotNull(targetProduct);
        assertEquals(new File(formatterConfig.getOutputFile()), targetProduct.getFileLocation());
        assertEquals(360, targetProduct.getSceneRasterWidth());
        assertEquals(180, targetProduct.getSceneRasterHeight());
        assertEquals("01-JAN-2002 00:00:00.000000", targetProduct.getStartTime().format());
        assertEquals("10-JAN-2002 00:00:00.000000", targetProduct.getEndTime().format());
        assertNotNull(targetProduct.getBand("num_obs"));
        assertNotNull(targetProduct.getBand("num_passes"));
        assertNotNull(targetProduct.getBand("chl_mean"));
        assertNotNull(targetProduct.getBand("chl_sigma"));

        int w = 4;
        int h = 4;
        float[] actualPixels = new float[w * h];
        float[] expectedPixels;
        float ___ = Float.NaN;

        targetProduct.getBand("chl_mean").readPixels(179, 91 - h, w, h, actualPixels);
        float mea = (obs1 + obs2 + obs3 + obs4 + obs5) / 5;
        expectedPixels = new float[]{
                ___, ___, ___, ___,
                ___, mea, mea, ___,
                ___, mea, mea, ___,
                ___, ___, ___, ___,
        };
        assertArrayEquals(expectedPixels, actualPixels, 1e-4F);

        targetProduct.getBand("chl_sigma").readPixels(179, 91 - h, w, h, actualPixels);
        float sig = (float) sqrt((obs1 * obs1 + obs2 * obs2 + obs3 * obs3 + obs4 * obs4 + obs5 * obs5) / 5 - mea * mea);
        expectedPixels = new float[]{
                ___, ___, ___, ___,
                ___, sig, sig, ___,
                ___, sig, sig, ___,
                ___, ___, ___, ___,
        };
        assertArrayEquals(expectedPixels, actualPixels, 1e-4F);

        targetProduct.dispose();
    }

    /**
     * The following configuration generates a 1-degree resolution local product (4 x 4 pixels) from 5 observations.
     * The local region is lon=-1..+3 and lat=-1..+3 degrees.
     * Values are only generated for pixels at x=1..2 and y=1..2.
     *
     * @throws Exception if something goes badly wrong
     * @see #testGlobalBinning()
     */
    @Test
    public void testLocalBinning() throws Exception {

        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final BinningOp binningOp = new BinningOp();

        binningOp.setSourceProducts(createSourceProduct(obs1),
                                    createSourceProduct(obs2),
                                    createSourceProduct(obs3),
                                    createSourceProduct(obs4),
                                    createSourceProduct(obs5));

        GeometryFactory gf = new GeometryFactory();
        binningOp.setRegion(gf.createPolygon(gf.createLinearRing(new Coordinate[]{
                new Coordinate(-1.0, -1.0),
                new Coordinate(3.0, -1.0),
                new Coordinate(3.0, 3.0),
                new Coordinate(-1.0, 3.0),
                new Coordinate(-1.0, -1.0),
        }), null));
        binningOp.setStartDate("2002-01-01");
        binningOp.setEndDate("2002-01-10");
        binningOp.setBinningConfig(binningConfig);
        binningOp.setFormatterConfig(formatterConfig);

        final Product targetProduct = binningOp.getTargetProduct();
        assertNotNull(targetProduct);
        assertEquals(new File(formatterConfig.getOutputFile()), targetProduct.getFileLocation());
        assertEquals(4, targetProduct.getSceneRasterWidth());
        assertEquals(4, targetProduct.getSceneRasterHeight());
        assertEquals("01-JAN-2002 00:00:00.000000", targetProduct.getStartTime().format());
        assertEquals("10-JAN-2002 00:00:00.000000", targetProduct.getEndTime().format());
        assertNotNull(targetProduct.getBand("num_obs"));
        assertNotNull(targetProduct.getBand("num_passes"));
        assertNotNull(targetProduct.getBand("chl_mean"));
        assertNotNull(targetProduct.getBand("chl_sigma"));

        int w = 4;
        int h = 4;
        float[] actualPixels = new float[w * h];
        float[] expectedPixels;
        float ___ = Float.NaN;

        targetProduct.getBand("chl_mean").readPixels(0, 0, w, h, actualPixels);
        float mea = (obs1 + obs2 + obs3 + obs4 + obs5) / 5;
        expectedPixels = new float[]{
                ___, ___, ___, ___,
                ___, mea, mea, ___,
                ___, mea, mea, ___,
                ___, ___, ___, ___,
        };
        assertArrayEquals(expectedPixels, actualPixels, 1e-4F);

        targetProduct.getBand("chl_sigma").readPixels(0, 0, w, h, actualPixels);
        float sig = (float) sqrt((obs1 * obs1 + obs2 * obs2 + obs3 * obs3 + obs4 * obs4 + obs5 * obs5) / 5 - mea * mea);
        expectedPixels = new float[]{
                ___, ___, ___, ___,
                ___, sig, sig, ___,
                ___, sig, sig, ___,
                ___, ___, ___, ___,
        };
        assertArrayEquals(expectedPixels, actualPixels, 1e-4F);

        targetProduct.dispose();
    }

    @Test
    public void testNoSourceProductSet() throws Exception {
        final BinningOp binningOp = new BinningOp();
        testThatOperatorExceptionIsThrown(binningOp, ".*single source product.*");
    }

    @Test
    public void testBinningConfigNotSet() throws Exception {
        final BinningOp binningOp = new BinningOp();
        binningOp.setSourceProduct(createSourceProduct());
        testThatOperatorExceptionIsThrown(binningOp, ".*parameter 'binningConfig'.*");
    }

    @Test
    public void testInvalidConfigsSet() throws Exception {
        final BinningOp binningOp = new BinningOp();
        binningOp.setSourceProduct(createSourceProduct());
        binningOp.setBinningConfig(new BinningConfig());        // not ok, numRows == 0
        testThatOperatorExceptionIsThrown(binningOp, ".*parameter 'binningConfig.maskExpr'.*");
    }

    @Test
    public void testNoStartDateSet() throws Exception {
        final BinningOp binningOp = new BinningOp();
        binningOp.setSourceProduct(createSourceProduct());
        binningOp.setBinningConfig(createBinningConfig());
        binningOp.setFormatterConfig(createFormatterConfig());
        testThatOperatorExceptionIsThrown(binningOp, ".*determine 'startDate'.*");
    }

    @Test
    public void testNoEndDateSet() throws Exception {
        final BinningOp binningOp = new BinningOp();
        binningOp.setSourceProduct(createSourceProduct());
        binningOp.setStartDate("2007-06-21");
        binningOp.setBinningConfig(createBinningConfig());
        binningOp.setFormatterConfig(createFormatterConfig());
        testThatOperatorExceptionIsThrown(binningOp, ".*determine 'endDate'.*");
    }

    private BinningConfig createBinningConfig() {
        final AggregatorConfig aggregatorConfig = new AggregatorConfig();
        aggregatorConfig.setAggregatorName("AVG");
        aggregatorConfig.setVarName("chl");
        final BinningConfig binningConfig = new BinningConfig();
        binningConfig.setAggregatorConfigs(aggregatorConfig);
        binningConfig.setNumRows(180);
        binningConfig.setMaskExpr("true");
        return binningConfig;
    }

    private FormatterConfig createFormatterConfig() throws IOException {
        final File targetFile = File.createTempFile("BinningOpTest-", ".dim");
        final FormatterConfig formatterConfig = new FormatterConfig();
        formatterConfig.setOutputFile(targetFile.getPath());
        formatterConfig.setOutputType("Product");
        formatterConfig.setOutputFormat("BEAM-DIMAP");
        return formatterConfig;
    }

    private Product createSourceProduct() {
        return createSourceProduct(1.0F);
    }

    private Product createSourceProduct(float value) {
        final Product p = new Product("P" + productCounter++, "T", 2, 2);
        final TiePointGrid latitude = new TiePointGrid("latitude", 2, 2, 0.5F, 0.5F, 1.0F, 1.0F, new float[]{
                1.0F, 1.0F,
                0.0F, 0.0F,
        });
        final TiePointGrid longitude = new TiePointGrid("longitude", 2, 2, 0.5F, 0.5F, 1.0F, 1.0F, new float[]{
                0.0F, 1.0F,
                0.0F, 1.0F,
        });
        p.addTiePointGrid(latitude);
        p.addTiePointGrid(longitude);
        p.setGeoCoding(new TiePointGeoCoding(latitude, longitude));
        p.addBand("chl", value + "");
        return p;
    }

    private void testThatOperatorExceptionIsThrown(BinningOp binningOp, String regex) {
        String message = "OperatorException expected with message regex: " + regex;
        try {
            binningOp.getTargetProduct();
            fail(message);
        } catch (OperatorException e) {
            assertTrue(message + ", got [" + e.getMessage() + "]", Pattern.matches(regex, e.getMessage()));
        }
    }


}
