package com.bc.calvalus.processing.beam;

import com.bc.ceres.core.ProgressMonitor;
import org.apache.commons.io.output.NullWriter;
import org.esa.snap.core.dataio.EncodeQualification;
import org.esa.snap.core.dataio.ProductIOPlugInManager;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNode;
import org.esa.snap.core.util.io.SnapFileFilter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

public class StreamingProductWriterTest {

    private static Map<String, float[]> result;

    @Before
    public void setUp() throws Exception {
        ProductIOPlugInManager.getInstance().addWriterPlugIn(getWriterPlugIn());

    }

    @Test
    public void testDifferentRasterSizes() throws Exception {
        final int PREFERRED_TILE_SIZE = 8;
        Product product = new Product("productName", "productWithDifferentBandSizes", 10, 10);
        product.setPreferredTileSize(PREFERRED_TILE_SIZE, PREFERRED_TILE_SIZE);

        Band largeBand = new Band("largeBand", ProductData.TYPE_FLOAT32, 10, 10);
        Band smallBand = new Band("smallBand", ProductData.TYPE_FLOAT32, 3, 3);

        float[] largeBandData = new float[10 * 10];
        float[] smallBandData = new float[3 * 3];

        for (int i = 0; i < 10 * 10; i++) {
            largeBandData[i] = i;
        }
        for (int i = 0; i < 3 * 3; i++) {
            smallBandData[i] = 10000 - i;
        }

        largeBand.setData(new ProductData.Float(largeBandData));
        smallBand.setData(new ProductData.Float(smallBandData));

        product.addBand(largeBand);
        product.addBand(smallBand);

        result = new HashMap<>();
        StreamingProductWriter.writeProductInSlices(product, new NullWriter(), "NullFormat", PREFERRED_TILE_SIZE, ProgressMonitor.NULL);

        float[] largeBandData1 = new float[10 * PREFERRED_TILE_SIZE];
        float[] largeBandData2 = new float[10 * 2];
        for (int i = 0; i < 10 * PREFERRED_TILE_SIZE; i++) {
            largeBandData1[i] = i;
        }
        for (int i = 0; i < 10 * 2; i++) {
            largeBandData2[i] = 10 * PREFERRED_TILE_SIZE + i;
        }

        float[] smallBandData1 = new float[3 * 3];
        for (int i = 0; i < 3 * 3; i++) {
            smallBandData1[i] = 10000 - i;
        }

        assertArrayEquals(result.get("largeBand(0)"), largeBandData1, 1E-5F);
        assertArrayEquals(result.get("largeBand(8)"), largeBandData2, 1E-5F);
        assertArrayEquals(result.get("smallBand(0)"), smallBandData1, 1E-5F);
    }

    @Test
    public void testEqualRasterSizes() throws Exception {
        final int WIDTH = 10;
        final int HEIGHT = 10;

        Product product = new Product("productName", "productWithDifferentBandSizes", WIDTH, HEIGHT);
        product.setPreferredTileSize(4, 8);

        Band band1 = new Band("band1", ProductData.TYPE_FLOAT32, WIDTH, HEIGHT);
        Band band2 = new Band("band2", ProductData.TYPE_FLOAT32, WIDTH, HEIGHT);

        float[] band1Data = new float[WIDTH * HEIGHT];
        float[] band2Data = new float[WIDTH * HEIGHT];

        for (int i = 0; i < WIDTH * HEIGHT; i++) {
            band1Data[i] = i;
        }
        for (int i = 0; i < WIDTH * HEIGHT; i++) {
            band2Data[i] = 10000 - i;
        }

        band1.setData(new ProductData.Float(band1Data));
        band2.setData(new ProductData.Float(band2Data));

        product.addBand(band1);
        product.addBand(band2);

        result = new HashMap<>();
        StreamingProductWriter.writeProductInSlices(product, new NullWriter(), "NullFormat", 10, ProgressMonitor.NULL);
    }


    // helper stuff
    private static ProductWriterPlugIn getWriterPlugIn() {
        return new ProductWriterPlugIn() {

            @Override
            public String[] getFormatNames() {
                return new String[] {"NullFormat"};
            }

            @Override
            public String[] getDefaultFileExtensions() {
                return new String[0];
            }

            @Override
            public String getDescription(Locale locale) {
                return null;
            }

            @Override
            public SnapFileFilter getProductFileFilter() {
                return null;
            }

            @Override
            public EncodeQualification getEncodeQualification(Product product) {
                return null;
            }

            @Override
            public Class[] getOutputTypes() {
                return new Class[0];
            }

            @Override
            public ProductWriter createWriterInstance() {
                return new ProductWriter() {
                    @Override
                    public ProductWriterPlugIn getWriterPlugIn() {
                        return null;
                    }

                    @Override
                    public Object getOutput() {
                        return null;
                    }

                    @Override
                    public void writeProductNodes(Product product, Object output) throws IOException {
                    }

                    @Override
                    public void writeBandRasterData(Band sourceBand, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) throws IOException {
                        result.put(sourceBand.getName() + "(" + sourceOffsetY + ")", (float[]) sourceBuffer.getElems());
                    }

                    @Override
                    public void flush() throws IOException {

                    }

                    @Override
                    public void close() throws IOException {

                    }

                    @Override
                    public boolean shouldWrite(ProductNode node) {
                        return true;
                    }

                    @Override
                    public boolean isIncrementalMode() {
                        return false;
                    }

                    @Override
                    public void setIncrementalMode(boolean enabled) {

                    }

                    @Override
                    public void deleteOutput() throws IOException {

                    }

                    @Override
                    public void removeBand(Band band) {

                    }

                    @Override
                    public void setFormatName(String formatName) {

                    }
                };
            }
        };
    }
}