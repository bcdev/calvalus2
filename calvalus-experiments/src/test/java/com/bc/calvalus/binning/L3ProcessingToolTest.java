package com.bc.calvalus.binning;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class L3ProcessingToolTest {
    static final float NAN = Float.NaN;

    @Test
    public void testPath() {
        assertEquals("part-r-00004", String.format("part-r-%05d", 4));
    }


    @Test
    public void testProcessBinRowCompleteEquator() {
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

        float[] imageData = new float[6 * 12];
        int y = 2;
        int width = 12;
        int height = 6;
        L3ProcessingTool.processBinRow0(binningGrid, y, binRow, imageData, width, height, true);

        assertEquals(11f, imageData[y * width + 0], 1E-5f);
        assertEquals(12f, imageData[y * width + 1], 1E-5f);
        assertEquals(13f, imageData[y * width + 2], 1E-5f);
        assertEquals(14f, imageData[y * width + 3], 1E-5f);
        assertEquals(15f, imageData[y * width + 4], 1E-5f);
        assertEquals(16f, imageData[y * width + 5], 1E-5f);
        assertEquals(17f, imageData[y * width + 6], 1E-5f);
        assertEquals(18f, imageData[y * width + 7], 1E-5f);
        assertEquals(19f, imageData[y * width + 8], 1E-5f);
        assertEquals(20f, imageData[y * width + 9], 1E-5f);
        assertEquals(21f, imageData[y * width + 10], 1E-5f);
        assertEquals(22f, imageData[y * width + 11], 1E-5f);


    }

    @Test
    public void testProcessBinRowIncompleteEquator() {
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

        float[] imageData = new float[6 * 12];
        int y = 2;
        int width = 12;
        int height = 6;
        L3ProcessingTool.processBinRow0(binningGrid, y, binRow, imageData, width, height, true);

        assertEquals(NAN, imageData[y * width + 0], 1E-5f);
        assertEquals(12f, imageData[y * width + 1], 1E-5f);
        assertEquals(13f, imageData[y * width + 2], 1E-5f);
        assertEquals(NAN, imageData[y * width + 3], 1E-5f);
        assertEquals(NAN, imageData[y * width + 4], 1E-5f);
        assertEquals(16f, imageData[y * width + 5], 1E-5f);
        assertEquals(NAN, imageData[y * width + 6], 1E-5f);
        assertEquals(18f, imageData[y * width + 7], 1E-5f);
        assertEquals(19f, imageData[y * width + 8], 1E-5f);
        assertEquals(20f, imageData[y * width + 9], 1E-5f);
        assertEquals(21f, imageData[y * width + 10], 1E-5f);
        assertEquals(NAN, imageData[y * width + 11], 1E-5f);


    }


    @Test
    public void testProcessBinRowCompletePolar() {
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

        float[] imageData = new float[6 * 12];
        int y = 0;
        int width = 12;
        int height = 6;
        L3ProcessingTool.processBinRow0(binningGrid, y, binRow, imageData, width, height, true);

        assertEquals(0f, imageData[y * width + 0], 1E-5f);
        assertEquals(0f, imageData[y * width + 1], 1E-5f);
        assertEquals(0f, imageData[y * width + 2], 1E-5f);
        assertEquals(0f, imageData[y * width + 3], 1E-5f);
        assertEquals(1f, imageData[y * width + 4], 1E-5f);
        assertEquals(1f, imageData[y * width + 5], 1E-5f);
        assertEquals(1f, imageData[y * width + 6], 1E-5f);
        assertEquals(1f, imageData[y * width + 7], 1E-5f);
        assertEquals(2f, imageData[y * width + 8], 1E-5f);
        assertEquals(2f, imageData[y * width + 9], 1E-5f);
        assertEquals(2f, imageData[y * width + 10], 1E-5f);
        assertEquals(2f, imageData[y * width + 11], 1E-5f);
    }

    @Test
    public void testProcessBinRowIncompletePolar() {
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

        float[] imageData = new float[6 * 12];
        int y = 0;
        int width = 12;
        int height = 6;
        L3ProcessingTool.processBinRow0(binningGrid, y, binRow, imageData, width, height, true);

        assertEquals(0f, imageData[y * width + 0], 1E-5f);
        assertEquals(0f, imageData[y * width + 1], 1E-5f);
        assertEquals(0f, imageData[y * width + 2], 1E-5f);
        assertEquals(0f, imageData[y * width + 3], 1E-5f);
        assertEquals(NAN, imageData[y * width + 4], 1E-5f);
        assertEquals(NAN, imageData[y * width + 5], 1E-5f);
        assertEquals(NAN, imageData[y * width + 6], 1E-5f);
        assertEquals(NAN, imageData[y * width + 7], 1E-5f);
        assertEquals(2f, imageData[y * width + 8], 1E-5f);
        assertEquals(2f, imageData[y * width + 9], 1E-5f);
        assertEquals(2f, imageData[y * width + 10], 1E-5f);
        assertEquals(2f, imageData[y * width + 11], 1E-5f);
    }

    @Test
    public void testProcessBinRowEmpty() {
        IsinBinningGrid binningGrid = new IsinBinningGrid(6);
        assertEquals(3, binningGrid.getNumCols(0));  //  0... 2 --> 0
        assertEquals(8, binningGrid.getNumCols(1));  //  3...10 --> 1
        assertEquals(12, binningGrid.getNumCols(2)); // 11...22 --> 2
        assertEquals(12, binningGrid.getNumCols(3));
        assertEquals(8, binningGrid.getNumCols(4));
        assertEquals(3, binningGrid.getNumCols(5));

        List<TemporalBin> binRow = Arrays.asList();

        float[] imageData = new float[6 * 12];
        int y = 0;
        int width = 12;
        int height = 6;
        L3ProcessingTool.processBinRow0(binningGrid, y, binRow, imageData, width, height, true);

        assertEquals(NAN, imageData[y * width + 0], 1E-5f);
        assertEquals(NAN, imageData[y * width + 1], 1E-5f);
        assertEquals(NAN, imageData[y * width + 2], 1E-5f);
        assertEquals(NAN, imageData[y * width + 3], 1E-5f);
        assertEquals(NAN, imageData[y * width + 4], 1E-5f);
        assertEquals(NAN, imageData[y * width + 5], 1E-5f);
        assertEquals(NAN, imageData[y * width + 6], 1E-5f);
        assertEquals(NAN, imageData[y * width + 7], 1E-5f);
        assertEquals(NAN, imageData[y * width + 8], 1E-5f);
        assertEquals(NAN, imageData[y * width + 9], 1E-5f);
        assertEquals(NAN, imageData[y * width + 10], 1E-5f);
        assertEquals(NAN, imageData[y * width + 11], 1E-5f);
    }

    private TemporalBin createTBin(int idx) {
        TemporalBin temporalBin = new TemporalBin(idx);
        temporalBin.numObs = idx;
        return temporalBin;
    }
}
