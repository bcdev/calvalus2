package com.bc.calvalus.experiments.format;

import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.Stx;
import org.junit.Ignore;
import org.junit.Test;

import javax.imageio.stream.FileImageInputStream;
import java.awt.image.Raster;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Ignore
public class LineInterleavedEnvisatReaderTest {
    private static final int ONE_MB = 1024 * 1024;

    @Test
    public void testEnvisatReader() throws IOException {
        File inputFile = new File("src/test/data/MER_RR__1P.N1");
        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        Product product = plugIn.createReaderInstance().readProductNodes(inputFile, null);

        File testDataDir = new File("target/testdata");
        testDataDir.mkdirs();
        File convertedFile = new File(testDataDir, "converted");
        convertedFile.deleteOnExit();
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(convertedFile), ONE_MB);
        FileConverter n1Converter = new N1ToLineInterleavedConverter();
        n1Converter.convertTo(inputFile, outputStream);
        outputStream.close();

        final ProductFile productFileLineInterleaved = ProductFile.open(convertedFile, new FileImageInputStream(convertedFile), true);
        final Product lineInterleavedProduct = plugIn.createReaderInstance().readProductNodes(productFileLineInterleaved, null);

        for (int i = 0; i < product.getNumBands(); i++) {
            Band b1 = product.getBandAt(i);
            Band b2 = lineInterleavedProduct.getBandAt(i);
            final Stx stx1 = b1.getStx();
            final Stx stx2 = b2.getStx();
            assertStxEquals(stx1, stx2, i);
            assertBandContentEquals(b1, b2);
        }
    }

    private static void assertStxEquals(Stx stx1, Stx stx2, int bandId) {
        assertEquals("Stx.min band[" + bandId + "]", stx1.getMin(), stx2.getMin(), 1e-6);
        assertEquals("Stx.max band[" + bandId + "]", stx1.getMax(), stx2.getMax(), 1e-6);
        assertEquals("Stx.mean band[" + bandId + "]", stx1.getMean(), stx2.getMean(), 1e-6);
        assertEquals("Stx.stdev band[" + bandId + "]", stx1.getStandardDeviation(), stx2.getStandardDeviation(), 1e-6);
        assertEquals("Stx.bincounts band[" + bandId + "]", stx1.getHistogramBinCount(), stx2.getHistogramBinCount());
        assertArrayEquals("Stx.bins. band[" + bandId + "]", stx1.getHistogramBins(), stx2.getHistogramBins());
    }

    private static void assertBandContentEquals(Band b1, Band b2) {
        final Raster data1 = b1.getSourceImage().getData();
        final Raster data2 = b2.getSourceImage().getData();
        assertEquals("width", data1.getWidth(), data2.getWidth());
        assertEquals("height", data1.getHeight(), data2.getHeight());

        for (int y = 0; y < data1.getHeight(); y++) {
            for (int x = 0; x < data1.getWidth(); x++) {
                assertEquals("sample x=" + x + " y=" + y, data1.getSample(x, y, 0), data2.getSample(x, y, 0));
            }
        }
    }

}
