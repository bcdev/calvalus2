package org.esa.beam.binning;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.TiePointGeoCoding;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.OperatorException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;

import static java.lang.Math.sqrt;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Norman Fomferra
 */
public class BinningOpTest {
     static {
         GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
     }

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
        try {
            assertGlobalBinningProductIsOk(targetProduct, new File(formatterConfig.getOutputFile()), obs1, obs2, obs3, obs4, obs5);
        } catch (Exception e) {
            targetProduct.dispose();
        }
    }

    /**
     * The following configuration generates a 1-degree resolution global product (360 x 180 pixels) from 5 observations.
     * Values are only generated for pixels at x=180..181 and y=87..89.
     *
     * @throws Exception if something goes badly wrong
     * @see #testLocalBinning()
     */
    @Test
    public void testGlobalBinningViaGPF() throws Exception {

        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final HashMap<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("startDate", "2002-01-01");
        parameters.put("endDate", "2002-01-10");
        parameters.put("binningConfig", binningConfig);
        parameters.put("formatterConfig", formatterConfig);

        final Product targetProduct = GPF.createProduct("Binning", parameters,
                                                        createSourceProduct(obs1),
                                                        createSourceProduct(obs2),
                                                        createSourceProduct(obs3),
                                                        createSourceProduct(obs4),
                                                        createSourceProduct(obs5));

        assertNotNull(targetProduct);
        try {
            assertGlobalBinningProductIsOk(targetProduct, new File(formatterConfig.getOutputFile()), obs1, obs2, obs3, obs4, obs5);
        } catch (Exception e) {
            targetProduct.dispose();
        }
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
        try {
            testLocalBinningProductIsOk(targetProduct, new File(formatterConfig.getOutputFile()), obs1, obs2, obs3, obs4, obs5);
        } catch (IOException e) {
            targetProduct.dispose();
        }
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
    public void testLocalBinningViaGPF() throws Exception {

        BinningConfig binningConfig = createBinningConfig();
        FormatterConfig formatterConfig = createFormatterConfig();

        float obs1 = 0.2F;
        float obs2 = 0.4F;
        float obs3 = 0.6F;
        float obs4 = 0.8F;
        float obs5 = 1.0F;

        final HashMap<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("region", "POLYGON((-1 -1, 3 -1, 3 3, -1 3, -1 -1))");
        parameters.put("startDate", "2002-01-01");
        parameters.put("endDate", "2002-01-10");
        parameters.put("binningConfig", binningConfig);
        parameters.put("formatterConfig", formatterConfig);

        final Product targetProduct = GPF.createProduct("Binning",
                                                        parameters,
                                                        createSourceProduct(obs1),
                                                        createSourceProduct(obs2),
                                                        createSourceProduct(obs3),
                                                        createSourceProduct(obs4),
                                                        createSourceProduct(obs5));
        assertNotNull(targetProduct);
        try {
            testLocalBinningProductIsOk(targetProduct, new File(formatterConfig.getOutputFile()), obs1, obs2, obs3, obs4, obs5);
        } catch (IOException e) {
            targetProduct.dispose();
        }
    }

    private void assertGlobalBinningProductIsOk(Product targetProduct, File location, float obs1, float obs2, float obs3, float obs4, float obs5) throws IOException {
        assertTargetProductIsOk(targetProduct, location, obs1, obs2, obs3, obs4, obs5, 360, 180, 179, 87);
    }

    private void testLocalBinningProductIsOk(Product targetProduct, File location, float obs1, float obs2, float obs3, float obs4, float obs5) throws IOException {
        assertTargetProductIsOk(targetProduct, location, obs1, obs2, obs3, obs4, obs5, 4, 4, 0, 0);
    }

    private void assertTargetProductIsOk(Product targetProduct, File location, float obs1, float obs2, float obs3, float obs4, float obs5, int sceneRasterWidth, int sceneRasterHeight, int x0, int y0) throws IOException {
        final int w = 4;
        final int h = 4;

        final int _o_ = -1;
        final float _x_ = Float.NaN;

        assertEquals(location, targetProduct.getFileLocation());
        assertEquals(sceneRasterWidth, targetProduct.getSceneRasterWidth());
        assertEquals(sceneRasterHeight, targetProduct.getSceneRasterHeight());
        assertEquals("01-JAN-2002 00:00:00.000000", targetProduct.getStartTime().format());
        assertEquals("10-JAN-2002 00:00:00.000000", targetProduct.getEndTime().format());
        assertNotNull(targetProduct.getBand("num_obs"));
        assertNotNull(targetProduct.getBand("num_passes"));
        assertNotNull(targetProduct.getBand("chl_mean"));
        assertNotNull(targetProduct.getBand("chl_sigma"));
        assertEquals(_o_, targetProduct.getBand("num_obs").getNoDataValue(), 1e-10);
        assertEquals(_o_, targetProduct.getBand("num_passes").getNoDataValue(), 1e-10);
        assertEquals(_x_, targetProduct.getBand("chl_mean").getNoDataValue(), 1e-10);
        assertEquals(_x_, targetProduct.getBand("chl_sigma").getNoDataValue(), 1e-10);

        // Test pixel values of band "num_obs"
        //
        final int nob = 5;
        final int[] expectedNobs = new int[]{
                _o_, _o_, _o_, _o_,
                _o_, nob, nob, _o_,
                _o_, nob, nob, _o_,
                _o_, _o_, _o_, _o_,
        };
        final int[] actualNobs = new int[w * h];
        targetProduct.getBand("num_obs").readPixels(x0, y0, w, h, actualNobs);
        assertArrayEquals(expectedNobs, actualNobs);

        // Test pixel values of band "num_passes"
        //
        final int npa = 5;
        final int[] expectedNpas = new int[]{
                _o_, _o_, _o_, _o_,
                _o_, npa, npa, _o_,
                _o_, npa, npa, _o_,
                _o_, _o_, _o_, _o_,
        };
        final int[] actualNpas = new int[w * h];
        targetProduct.getBand("num_passes").readPixels(x0, y0, w, h, actualNpas);
        assertArrayEquals(expectedNpas, actualNpas);

        // Test pixel values of band "chl_mean"
        //
        final float mea = (obs1 + obs2 + obs3 + obs4 + obs5) / nob;
        final float[] expectedMeas = new float[]{
                _x_, _x_, _x_, _x_,
                _x_, mea, mea, _x_,
                _x_, mea, mea, _x_,
                _x_, _x_, _x_, _x_,
        };
        final float[] actualMeas = new float[w * h];
        targetProduct.getBand("chl_mean").readPixels(x0, y0, w, h, actualMeas);
        assertArrayEquals(expectedMeas, actualMeas, 1e-4F);

        // Test pixel values of band "chl_sigma"
        //
        final float sig = (float) sqrt((obs1 * obs1 + obs2 * obs2 + obs3 * obs3 + obs4 * obs4 + obs5 * obs5) / nob - mea * mea);
        final float[] expectedSigs = new float[]{
                _x_, _x_, _x_, _x_,
                _x_, sig, sig, _x_,
                _x_, sig, sig, _x_,
                _x_, _x_, _x_, _x_,
        };
        final float[] actualSigs = new float[w * h];
        targetProduct.getBand("chl_sigma").readPixels(x0, y0, w, h, actualSigs);
        assertArrayEquals(expectedSigs, actualSigs, 1e-4F);
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

    static int sourceProductCounter = 1;

    private Product createSourceProduct() {
        return createSourceProduct(1.0F);
    }

    private Product createSourceProduct(float value) {
        final Product p = new Product("P" + sourceProductCounter++, "T", 2, 2);
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
