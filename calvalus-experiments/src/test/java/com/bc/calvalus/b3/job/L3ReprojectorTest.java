package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinManagerImpl;
import com.bc.calvalus.b3.BinningContextImpl;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.TemporalBin;
import com.bc.calvalus.b3.VariableContextImpl;
import com.bc.calvalus.b3.WritableVector;
import com.bc.ceres.glevel.MultiLevelImage;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.junit.Before;
import org.junit.Test;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class L3ReprojectorTest {
    static final float NAN = Float.NaN;
    private BinManager binManager;

    @Before
    public void init() {
        AggregatorAverage aggregator = new AggregatorAverage(new VariableContextImpl(), "ndvi");
        binManager = new BinManagerImpl(aggregator);
    }

    @Test
    public void testPath() {
        assertEquals("part-r-00004", String.format("part-r-%05d", 4));
    }

    @Test
    public void testProcessBinRowCompleteEquator() throws Exception {
        IsinBinningGrid binningGrid = new IsinBinningGrid(6);
        assertEquals(3, binningGrid.getNumCols(0));  //  0... 2 --> 0
        assertEquals(8, binningGrid.getNumCols(1));  //  3...10 --> 1
        assertEquals(12, binningGrid.getNumCols(2)); // 11...22 --> 2
        assertEquals(12, binningGrid.getNumCols(3));
        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        List<TemporalBin> binRow = Arrays.asList(createTBin(11),
                                                 createTBin(12),
                                                 createTBin(13),
                                                 createTBin(14),
                                                 createTBin(15),
                                                 createTBin(16),
                                                 createTBin(17),
                                                 createTBin(18),
                                                 createTBin(19),
                                                 createTBin(20),
                                                 createTBin(21),
                                                 createTBin(22)
        );

        int y = 2;
        int width = 12;
        int height = 6;
        MyTemporalBinProcessor binProcessor = new MyTemporalBinProcessor(width, height);
        L3Reprojector.reprojectRow(getCtx(binningGrid), new Rectangle(width, height), y, binRow, binProcessor, width, height);
        float[] nobsData = binProcessor.nobsData;
        float[] meanData = binProcessor.meanData;
        float[] sigmaData = binProcessor.sigmaData;

        assertEquals(11f, nobsData[y * width + 0], 1E-5f);
        assertEquals(12f, nobsData[y * width + 1], 1E-5f);
        assertEquals(13f, nobsData[y * width + 2], 1E-5f);
        assertEquals(14f, nobsData[y * width + 3], 1E-5f);
        assertEquals(15f, nobsData[y * width + 4], 1E-5f);
        assertEquals(16f, nobsData[y * width + 5], 1E-5f);
        assertEquals(17f, nobsData[y * width + 6], 1E-5f);
        assertEquals(18f, nobsData[y * width + 7], 1E-5f);
        assertEquals(19f, nobsData[y * width + 8], 1E-5f);
        assertEquals(20f, nobsData[y * width + 9], 1E-5f);
        assertEquals(21f, nobsData[y * width + 10], 1E-5f);
        assertEquals(22f, nobsData[y * width + 11], 1E-5f);


    }

    @Test
    public void testProcessBinRowIncompleteEquator() throws Exception {
        IsinBinningGrid binningGrid = new IsinBinningGrid(6);
        assertEquals(3, binningGrid.getNumCols(0));  //  0... 2 --> 0
        assertEquals(8, binningGrid.getNumCols(1));  //  3...10 --> 1
        assertEquals(12, binningGrid.getNumCols(2)); // 11...22 --> 2
        assertEquals(12, binningGrid.getNumCols(3));
        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        List<TemporalBin> binRow = Arrays.asList(//createTBin(11),
                                                 createTBin(12),
                                                 createTBin(13),
                                                 // createTBin(14),
                                                 // createTBin(15),
                                                 createTBin(16),
                                                 // createTBin(17),
                                                 createTBin(18),
                                                 createTBin(19),
                                                 createTBin(20),
                                                 createTBin(21)
                                                 // createTBin(22)
        );

        int y = 2;
        int width = 12;
        int height = 6;
        MyTemporalBinProcessor binProcessor = new MyTemporalBinProcessor(width, height);
        L3Reprojector.reprojectRow(getCtx(binningGrid), new Rectangle(width, height), y, binRow, binProcessor, width, height);
        float[] nobsData = binProcessor.nobsData;

        assertEquals(NAN, nobsData[y * width + 0], 1E-5f);
        assertEquals(12f, nobsData[y * width + 1], 1E-5f);
        assertEquals(13f, nobsData[y * width + 2], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 3], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 4], 1E-5f);
        assertEquals(16f, nobsData[y * width + 5], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 6], 1E-5f);
        assertEquals(18f, nobsData[y * width + 7], 1E-5f);
        assertEquals(19f, nobsData[y * width + 8], 1E-5f);
        assertEquals(20f, nobsData[y * width + 9], 1E-5f);
        assertEquals(21f, nobsData[y * width + 10], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 11], 1E-5f);


    }


    @Test
    public void testProcessBinRowCompletePolar() throws Exception {
        IsinBinningGrid binningGrid = new IsinBinningGrid(6);
        assertEquals(3, binningGrid.getNumCols(0));  //  0... 2 --> 0
        assertEquals(8, binningGrid.getNumCols(1));  //  3...10 --> 1
        assertEquals(12, binningGrid.getNumCols(2)); // 11...22 --> 2
        assertEquals(12, binningGrid.getNumCols(3));
        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        List<TemporalBin> binRow = Arrays.asList(createTBin(0),
                                                 createTBin(1),
                                                 createTBin(2));

        int y = 0;
        int width = 12;
        int height = 6;
        MyTemporalBinProcessor binProcessor = new MyTemporalBinProcessor(width, height);
        L3Reprojector.reprojectRow(getCtx(binningGrid), new Rectangle(width, height), y, binRow, binProcessor, width, height);
        float[] nobsData = binProcessor.nobsData;
        float[] meanData = binProcessor.meanData;
        float[] sigmaData = binProcessor.sigmaData;

        assertEquals(0f, nobsData[y * width + 0], 1E-5f);
        assertEquals(0f, nobsData[y * width + 1], 1E-5f);
        assertEquals(0f, nobsData[y * width + 2], 1E-5f);
        assertEquals(0f, nobsData[y * width + 3], 1E-5f);
        assertEquals(1f, nobsData[y * width + 4], 1E-5f);
        assertEquals(1f, nobsData[y * width + 5], 1E-5f);
        assertEquals(1f, nobsData[y * width + 6], 1E-5f);
        assertEquals(1f, nobsData[y * width + 7], 1E-5f);
        assertEquals(2f, nobsData[y * width + 8], 1E-5f);
        assertEquals(2f, nobsData[y * width + 9], 1E-5f);
        assertEquals(2f, nobsData[y * width + 10], 1E-5f);
        assertEquals(2f, nobsData[y * width + 11], 1E-5f);
    }

    @Test
    public void testProcessBinRowIncompletePolar() throws Exception {
        IsinBinningGrid binningGrid = new IsinBinningGrid(6);
        assertEquals(3, binningGrid.getNumCols(0));  //  0... 2 --> 0
        assertEquals(8, binningGrid.getNumCols(1));  //  3...10 --> 1
        assertEquals(12, binningGrid.getNumCols(2)); // 11...22 --> 2
        assertEquals(12, binningGrid.getNumCols(3));
        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        List<TemporalBin> binRow = Arrays.asList(createTBin(0),
                                                 //createTBin(1),
                                                 createTBin(2));

        int y = 0;
        int width = 12;
        int height = 6;
        MyTemporalBinProcessor binProcessor = new MyTemporalBinProcessor(width, height);
        L3Reprojector.reprojectRow(getCtx(binningGrid), new Rectangle(width, height), y, binRow, binProcessor, width, height);
        float[] nobsData = binProcessor.nobsData;

        assertEquals(0f, nobsData[y * width + 0], 1E-5f);
        assertEquals(0f, nobsData[y * width + 1], 1E-5f);
        assertEquals(0f, nobsData[y * width + 2], 1E-5f);
        assertEquals(0f, nobsData[y * width + 3], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 4], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 5], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 6], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 7], 1E-5f);
        assertEquals(2f, nobsData[y * width + 8], 1E-5f);
        assertEquals(2f, nobsData[y * width + 9], 1E-5f);
        assertEquals(2f, nobsData[y * width + 10], 1E-5f);
        assertEquals(2f, nobsData[y * width + 11], 1E-5f);
    }

    @Test
    public void testProcessBinRowEmpty() throws Exception {
        IsinBinningGrid binningGrid = new IsinBinningGrid(6);
        assertEquals(3, binningGrid.getNumCols(0));  //  0... 2 --> 0
        assertEquals(8, binningGrid.getNumCols(1));  //  3...10 --> 1
        assertEquals(12, binningGrid.getNumCols(2)); // 11...22 --> 2
        assertEquals(12, binningGrid.getNumCols(3));
        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        List<TemporalBin> binRow = Arrays.asList();

        int y = 0;
        int width = 12;
        int height = 6;
        MyTemporalBinProcessor binProcessor = new MyTemporalBinProcessor(width, height);
        L3Reprojector.reprojectRow(getCtx(binningGrid), new Rectangle(width, height), y, binRow, binProcessor, width, height);
        float[] nobsData = binProcessor.nobsData;

        assertEquals(NAN, nobsData[y * width + 0], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 1], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 2], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 3], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 4], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 5], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 6], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 7], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 8], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 9], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 10], 1E-5f);
        assertEquals(NAN, nobsData[y * width + 11], 1E-5f);
    }

    @Test
    public void testThatProductCanBeTiledInSlices() throws IOException {
        File input = new File("testdata/MER_RR__1P_TEST.N1");
        if (!input.exists()) {
            System.out.println("Warning: test not performed: can't find " + input);
            return;
        }

        System.setProperty("beam.envisat.tileHeight", Integer.toString(64));
        EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        ProductReader productReader = plugIn.createReaderInstance();

        Product sourceProduct = productReader.readProductNodes(input, null);
        Band band = sourceProduct.getBand("radiance_13");
        assertThatImageIsSliced(sourceProduct, band.getSourceImage());
        assertThatImageIsSliced(sourceProduct, band.getValidMaskImage());
        assertThatImageIsSliced(sourceProduct, band.getGeophysicalImage());
    }

    private void assertThatImageIsSliced(Product product, MultiLevelImage image) {
        int tileWidth = image.getTileWidth();
        int sceneRasterWidth = product.getSceneRasterWidth();
        String msg = MessageFormat.format("Product not sliced: image.tileSize = {0}x{1}, product.sceneRasterSize = {2}x{3}",
                                          tileWidth, image.getTileHeight(), sceneRasterWidth, product.getSceneRasterHeight());
        assertTrue(msg, tileWidth == sceneRasterWidth);
    }

    private TemporalBin createTBin(int idx) {
        TemporalBin temporalBin = binManager.createTemporalBin(idx);
        temporalBin.setNumObs(idx);
        return temporalBin;
    }

    private BinningContextImpl getCtx(IsinBinningGrid binningGrid) {
        return new BinningContextImpl(binningGrid, new VariableContextImpl(), binManager);
    }

    private static class MyTemporalBinProcessor extends L3Reprojector.TemporalBinProcessor {
        float[] nobsData;
        float[] meanData;
        float[] sigmaData;
        private final int w;
        private final int h;

        private MyTemporalBinProcessor(int w, int h) {
            this.w = w;
            this.h = h;
            nobsData = new float[w * h];
            meanData = new float[w * h];
            sigmaData = new float[w * h];
        }

        @Override
        public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception {
            nobsData[y * w + x] = temporalBin.getNumObs();
            meanData[y * w + x] = outputVector.get(0);
            sigmaData[y * w + x] = outputVector.get(1);
        }

        @Override
        public void processMissingBin(int x, int y) throws Exception {
            nobsData[y * w + x] = Float.NaN;
            meanData[y * w + x] = Float.NaN;
            sigmaData[y * w + x] = Float.NaN;
        }
    }
}
