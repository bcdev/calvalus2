package org.esa.beam.binning;

import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.TiePointGeoCoding;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.esa.beam.framework.gpf.OperatorException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author Norman Fomferra
 */
public class BinningOpTest {
    int productCounter = 1;

    @Test (expected = OperatorException.class)
    public void testConfigsNotSet() throws Exception {
        final BinningOp binningOp = new BinningOp();
        binningOp.setSourceProducts(createSourceProduct(),
                                    createSourceProduct(),
                                    createSourceProduct());
        binningOp.getTargetProduct();
    }

    @Test(expected = OperatorException.class)
    public void testInvalidConfigsSet() throws Exception {
        final BinningOp binningOp = new BinningOp();
        binningOp.setSourceProducts(createSourceProduct(),
                                    createSourceProduct(),
                                    createSourceProduct());
        binningOp.setBinningConfig(new BinningConfig());        // not ok, numRows == 0
        binningOp.setFormatterConfig(createFormatterConfig());  // ok
        binningOp.getTargetProduct();
    }

    @Test
    public void testConfigsSet() throws Exception {
        final BinningOp binningOp = new BinningOp();
        binningOp.setSourceProducts(createSourceProduct(),
                                    createSourceProduct(),
                                    createSourceProduct());
        binningOp.setBinningConfig(createBinningConfig());
        binningOp.setFormatterConfig(createFormatterConfig());
        binningOp.getTargetProduct();
    }

    @Test
    public void testBinningOf3Products() throws Exception {
        final BinningOp binningOp = new BinningOp();
        binningOp.setSourceProducts(createSourceProduct(),
                                    createSourceProduct(),
                                    createSourceProduct());
        
        final BinningConfig binningConfig = createBinningConfig();
        binningOp.setBinningConfig(binningConfig);

        final FormatterConfig formatterConfig = createFormatterConfig();
        binningOp.setFormatterConfig(formatterConfig);

        final Product targetProduct = binningOp.getTargetProduct();
        assertNotNull(targetProduct);
        assertEquals(360, targetProduct.getSceneRasterWidth());
        assertEquals(180, targetProduct.getSceneRasterHeight());
        assertNotNull(targetProduct.getBand("chl_mean"));
        assertNotNull(targetProduct.getBand("chl_sigma"));
        assertEquals(new File(formatterConfig.getOutputFile()), targetProduct.getFileLocation());
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
        p.addBand("chl", productCounter + "/10.0");
        return p;
    }
}
