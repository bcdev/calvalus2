package com.bc.calvalus.binning;

import org.junit.Before;
import org.junit.Test;

import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.bc.calvalus.binning.TemporalBinReprojector.*;
import static org.junit.Assert.*;

/**
 * @author MarcoZ
 * @author Norman
 */
public class TemporalBinReprojectorTest {
    static final int NAN = -1;
    private BinManager binManager = new BinManagerImpl();
    private IsinBinningGrid binningGrid;
    private Rectangle rectangle;
    private NobsRaster raster;
    private int width;
    private int height;

    @Before
    public void setUp() throws Exception {
        binningGrid = new IsinBinningGrid(6);
        assertEquals(46, binningGrid.getNumBins());

        assertEquals(3, binningGrid.getNumCols(0));  //  0... 2 --> 0
        assertEquals(8, binningGrid.getNumCols(1));  //  3...10 --> 1
        assertEquals(12, binningGrid.getNumCols(2)); // 11...22 --> 2
        assertEquals(12, binningGrid.getNumCols(3)); // 23...34 --> 3
        assertEquals(8, binningGrid.getNumCols(4));  // 35...42 --> 4
        assertEquals(3, binningGrid.getNumCols(5));  // 43...45 --> 5

        width = 2 * binningGrid.getNumRows();
        height = binningGrid.getNumRows();
        assertEquals(12, width);
        assertEquals(6, height);

        rectangle = new Rectangle(width, height);
        raster = new NobsRaster(width, height);
        assertEquals("" +
                             "------------\n" +
                             "------------\n" +
                             "------------\n" +
                             "------------\n" +
                             "------------\n" +
                             "------------\n",
                     raster.toString());
    }

    @Test
    public void testReprojectPart_Full() throws Exception {

        ArrayList<TemporalBin> bins = new ArrayList<TemporalBin>();
        for (int i = 0; i < binningGrid.getNumBins(); i++) {
            bins.add(createTBin(i));
        }

        reprojectPart(getCtx(binningGrid), rectangle, bins.iterator(), raster);

        assertEquals("" +
                             "************\n" +
                             "************\n" +
                             "************\n" +
                             "************\n" +
                             "************\n" +
                             "************\n",
                     raster.toString());
    }

    @Test
    public void testReprojectPart_Alternating() throws Exception {

        ArrayList<TemporalBin> bins = new ArrayList<TemporalBin>();
        for (int i = 0; i < binningGrid.getNumBins(); i++) {
            bins.add(createTBin(i));
            i++;  // SKIP!!!
        }

        reprojectPart(getCtx(binningGrid), rectangle, bins.iterator(), raster);

        assertEquals("" +
                             "****++++****\n" +
                             "+**+**+**+**\n" +
                             "+*+*+*+*+*+*\n" +
                             "+*+*+*+*+*+*\n" +
                             "+**+**+**+**\n" +
                             "++++****++++\n",
                     raster.toString());
    }

    @Test
    public void testReprojectPart_BottomMissing() throws Exception {

        ArrayList<TemporalBin> bins = new ArrayList<TemporalBin>();
        for (int i = 0; i < 25; i++) {  // only up to 25!!!
            bins.add(createTBin(i));
        }

        reprojectPart(getCtx(binningGrid), rectangle, bins.iterator(), raster);

        assertEquals("" +
                             "************\n" +
                             "************\n" +
                             "************\n" +
                             "**++++++++++\n" +
                             "++++++++++++\n" +
                             "++++++++++++\n",
                     raster.toString());
    }

    @Test
    public void testReprojectPart_TopMissing() throws Exception {

        ArrayList<TemporalBin> bins = new ArrayList<TemporalBin>();
        for (int i = 15; i < binningGrid.getNumBins(); i++) {  // from 15 on!!!
            bins.add(createTBin(i));
        }

        reprojectPart(getCtx(binningGrid), rectangle, bins.iterator(), raster);

        assertEquals("" +
                             "++++++++++++\n" +
                             "++++++++++**\n" +
                             "************\n" +
                             "************\n" +
                             "************\n" +
                             "************\n",
                     raster.toString());
    }

    @Test
    public void testReprojectPart_SomeLinesMissing() throws Exception {

        ArrayList<TemporalBin> bins = new ArrayList<TemporalBin>();
        for (int i = 0; i < binningGrid.getNumBins(); i++) {  // from 15 on!!!
            if (!(binningGrid.getRowIndex(i) == 2 || binningGrid.getRowIndex(i) == 4)) {
                bins.add(createTBin(i));
            }
        }

        reprojectPart(getCtx(binningGrid), rectangle, bins.iterator(), raster);

        assertEquals("" +
                             "************\n" +
                             "************\n" +
                             "++++++++++++\n" +
                             "************\n" +
                             "++++++++++++\n" +
                             "************\n",
                     raster.toString());
    }


    @Test
    public void testProcessBinRowCompleteEquator() throws Exception {

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
        reprojectRow(getCtx(binningGrid), rectangle, y, binRow, raster, width, height);
        int[] nobsData = raster.nobsData;

        assertEquals(11, nobsData[y * width + 0]);
        assertEquals(12, nobsData[y * width + 1]);
        assertEquals(13, nobsData[y * width + 2]);
        assertEquals(14, nobsData[y * width + 3]);
        assertEquals(15, nobsData[y * width + 4]);
        assertEquals(16, nobsData[y * width + 5]);
        assertEquals(17, nobsData[y * width + 6]);
        assertEquals(18, nobsData[y * width + 7]);
        assertEquals(19, nobsData[y * width + 8]);
        assertEquals(20, nobsData[y * width + 9]);
        assertEquals(21, nobsData[y * width + 10]);
        assertEquals(22, nobsData[y * width + 11]);


    }

    @Test
    public void testProcessBinRowIncompleteEquator() throws Exception {

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
        reprojectRow(getCtx(binningGrid), rectangle, y, binRow, raster, width, height);
        int[] nobsData = raster.nobsData;

        assertEquals(NAN, nobsData[y * width + 0]);
        assertEquals(12, nobsData[y * width + 1]);
        assertEquals(13, nobsData[y * width + 2]);
        assertEquals(NAN, nobsData[y * width + 3]);
        assertEquals(NAN, nobsData[y * width + 4]);
        assertEquals(16, nobsData[y * width + 5]);
        assertEquals(NAN, nobsData[y * width + 6]);
        assertEquals(18, nobsData[y * width + 7]);
        assertEquals(19, nobsData[y * width + 8]);
        assertEquals(20, nobsData[y * width + 9]);
        assertEquals(21, nobsData[y * width + 10]);
        assertEquals(NAN, nobsData[y * width + 11]);


    }


    @Test
    public void testProcessBinRowCompletePolar() throws Exception {

        List<TemporalBin> binRow = Arrays.asList(createTBin(0),
                                                 createTBin(1),
                                                 createTBin(2));

        int y = 0;
        reprojectRow(getCtx(binningGrid), rectangle, y, binRow, raster, width, height);
        int[] nobsData = raster.nobsData;

        assertEquals(0, nobsData[y * width + 0]);
        assertEquals(0, nobsData[y * width + 1]);
        assertEquals(0, nobsData[y * width + 2]);
        assertEquals(0, nobsData[y * width + 3]);
        assertEquals(1, nobsData[y * width + 4]);
        assertEquals(1, nobsData[y * width + 5]);
        assertEquals(1, nobsData[y * width + 6]);
        assertEquals(1, nobsData[y * width + 7]);
        assertEquals(2, nobsData[y * width + 8]);
        assertEquals(2, nobsData[y * width + 9]);
        assertEquals(2, nobsData[y * width + 10]);
        assertEquals(2, nobsData[y * width + 11]);
    }

    @Test
    public void testProcessBinRowIncompletePolar() throws Exception {

        List<TemporalBin> binRow = Arrays.asList(createTBin(0),
                                                 //createTBin(1),
                                                 createTBin(2));

        int y = 0;
        reprojectRow(getCtx(binningGrid), rectangle, y, binRow, raster, width, height);
        int[] nobsData = raster.nobsData;

        assertEquals(0, nobsData[y * width + 0]);
        assertEquals(0, nobsData[y * width + 1]);
        assertEquals(0, nobsData[y * width + 2]);
        assertEquals(0, nobsData[y * width + 3]);
        assertEquals(NAN, nobsData[y * width + 4]);
        assertEquals(NAN, nobsData[y * width + 5]);
        assertEquals(NAN, nobsData[y * width + 6]);
        assertEquals(NAN, nobsData[y * width + 7]);
        assertEquals(2, nobsData[y * width + 8]);
        assertEquals(2, nobsData[y * width + 9]);
        assertEquals(2, nobsData[y * width + 10]);
        assertEquals(2, nobsData[y * width + 11]);
    }

    @Test
    public void testProcessBinRowEmpty() throws Exception {

        List<TemporalBin> binRow = Arrays.asList();

        int y = 0;
        reprojectRow(getCtx(binningGrid), rectangle, y, binRow, raster, width, height);
        int[] nobsData = raster.nobsData;

        assertEquals(NAN, nobsData[y * width + 0]);
        assertEquals(NAN, nobsData[y * width + 1]);
        assertEquals(NAN, nobsData[y * width + 2]);
        assertEquals(NAN, nobsData[y * width + 3]);
        assertEquals(NAN, nobsData[y * width + 4]);
        assertEquals(NAN, nobsData[y * width + 5]);
        assertEquals(NAN, nobsData[y * width + 6]);
        assertEquals(NAN, nobsData[y * width + 7]);
        assertEquals(NAN, nobsData[y * width + 8]);
        assertEquals(NAN, nobsData[y * width + 9]);
        assertEquals(NAN, nobsData[y * width + 10]);
        assertEquals(NAN, nobsData[y * width + 11]);
    }

    /*
     * Creates a test bin whose #obs = ID.
     */
    private TemporalBin createTBin(int idx) {
        TemporalBin temporalBin = binManager.createTemporalBin(idx);
        temporalBin.setNumObs(idx);
        return temporalBin;
    }

    private BinningContextImpl getCtx(IsinBinningGrid binningGrid) {
        return new BinningContextImpl(binningGrid, new VariableContextImpl(), binManager);
    }

    private static class NobsRaster extends TemporalBinProcessor {
        int[] nobsData;
        private final int w;

        private NobsRaster(int w, int h) {
            this.w = w;
            nobsData = new int[w * h];
            Arrays.fill(nobsData, -2);
        }

        @Override
        public void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception {
            nobsData[y * w + x] = temporalBin.getNumObs();
        }

        @Override
        public void processMissingBin(int x, int y) throws Exception {
            nobsData[y * w + x] = -1;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < nobsData.length; i++) {
                int d = nobsData[i];
                if (d == -2) {
                    // Never visited
                    sb.append('-');
                } else if (d == -1) {
                    // Visited, but missing data
                    sb.append("+");
                } else {
                    // Visited, valid data
                    sb.append("*");
                }
                if ((i + 1) % w == 0) {
                    sb.append('\n');
                }
            }
            return sb.toString();
        }
    }
}
