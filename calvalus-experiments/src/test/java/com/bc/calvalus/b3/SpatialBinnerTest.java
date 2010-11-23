package com.bc.calvalus.b3;


import org.junit.Test;

import java.awt.geom.AffineTransform;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.lang.Math.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;


public class SpatialBinnerTest {
    static final int SUM_X = 0;
    static final int SUM_XX = 1;
    static final int WEIGHT = 2;

    @Test
    public void testThatObservationsAreAggregated() throws Exception {

        VariableContext variableContext = new MyVariableContext("x");
        MyBinningGrid binningGrid = new MyBinningGrid();
        MyBinManager binningManager = new MyBinManager(new AggregatorAverageML(variableContext, "x"));
        TemporalBinner temporalBinner = new TemporalBinner(binningManager);
        SpatialBinner spatialBinner = new SpatialBinner(binningGrid,
                                                        binningManager,
                                                        temporalBinner, 2);

        spatialBinner.processObservationSlice(new ObservationImpl(0, 1.1, 1.1f),
                                              new ObservationImpl(0, 1.1, 1.2f),
                                              new ObservationImpl(0, 2.1, 1.3f),
                                              new ObservationImpl(0, 2.1, 1.4f));

        spatialBinner.processObservationSlice(new ObservationImpl(0, 1.1, 2.1f),
                                              new ObservationImpl(0, 2.1, 2.2f),
                                              new ObservationImpl(0, 2.1, 2.3f),
                                              new ObservationImpl(0, 2.1, 2.4f),
                                              new ObservationImpl(0, 3.1, 2.5f));

        assertEquals(3, temporalBinner.binMap.size());

        TemporalBin tbin1 = temporalBinner.binMap.get(1);
        assertNotNull(tbin1);
        assertEquals(3, tbin1.getNumObs());
        assertEquals(1, tbin1.getNumPasses());
        Vector agg1 = binningManager.getTemporalVector(tbin1, 0);
        assertNotNull(agg1);
        assertEquals(sqrt(3),
                     agg1.get(WEIGHT), 1e-5);
        assertEquals((log(1.1) + log(1.2) + log(2.1)) / sqrt(3),
                     agg1.get(SUM_X), 1e-5);

        TemporalBin tbin2 = temporalBinner.binMap.get(2);
        assertNotNull(tbin2);
        assertEquals(5, tbin2.getNumObs());
        assertEquals(1, tbin2.getNumPasses());
        Vector agg2 = binningManager.getTemporalVector(tbin2, 0);
        assertNotNull(agg2);
        assertEquals(sqrt(5),
                     agg2.get(WEIGHT), 1e-5);
        assertEquals((log(1.3) + log(1.4) + log(2.2) + log(2.3) + log(2.4)) / sqrt(5),
                     agg2.get(SUM_X), 1e-5);

        TemporalBin tbin3 = temporalBinner.binMap.get(3);
        assertNotNull(tbin3);
        assertEquals(1, tbin3.getNumObs());
        assertEquals(1, tbin3.getNumPasses());
        Vector agg3 = binningManager.getTemporalVector(tbin3, 0);
        assertNotNull(agg3);
        assertEquals(1.0,
                     agg3.get(WEIGHT), 1e-10);
        assertEquals(log(2.5),
                     agg3.get(SUM_X), 1e-5);
    }

    @Test
    public void testThatCellsAreDeterminedCorrectly() throws Exception {
        IsinBinningGrid grid = new IsinBinningGrid();

        // bin size in degree
        double binEdgeSize = 180.0 / grid.getNumRows();

        // we want 4 x 4 pixels per bin
        int pixelsPerBinEdge = 4;

        // we want 4 x 4 pixels per bin
        double pixelEdgeSize = binEdgeSize / pixelsPerBinEdge;

        // we want w x h pixels total --> num bins expected = w x h / (pixelsPerBinEdge*pixelsPerBinEdge)
        int w = 16;
        int h = 16;

        double rotAngle = 0.0;
        double lonRange = w * pixelEdgeSize;
        double latRange = h * pixelEdgeSize;
        ObservationImpl[][] pixelSlices = new ObservationImpl[h][w];
        AffineTransform at = AffineTransform.getRotateInstance(Math.toRadians(rotAngle));
        for (int j = 0; j < h; j++) {
            for (int i = 0; i < w; i++) {
                double lat = 0.5f * pixelEdgeSize + latRange * ((double) j / (double) h - 0.5);
                double lon = 0.5f * pixelEdgeSize + lonRange * ((double) i / (double) w - 0.5);
                double[] srcPts = new double[]{lat, lon};
                double[] dstPts = new double[2];
                at.transform(srcPts, 0, dstPts, 0, 1);
                pixelSlices[j][i] = new ObservationImpl(dstPts[0], dstPts[1]);
            }
        }

        VariableContext variableContext = new MyVariableContext("x");
        MyBinManager binningManager = new MyBinManager(new AggregatorAverageML(variableContext, "x"));
        MyBinProcessor binProcessor = new MyBinProcessor();
        SpatialBinner spatialBinner = new SpatialBinner(grid, binningManager, binProcessor, h);

        for (ObservationImpl[] pixelSlice : pixelSlices) {
            spatialBinner.processObservationSlice(pixelSlice);
        }

        ArrayList<SpatialBin> producedSpatialBins = binningManager.producedSpatialBins;

        int numObs = w * h;
        assertEquals(256, numObs);
        assertEquals(256, binProcessor.numObservationsTotal);
        for (int i = 0; i < 16; i++) {
            assertEquals(String.format("Problem with bin[%d]", i), 16, producedSpatialBins.get(i).getNumObs());
        }

        int numBins = w * h / (pixelsPerBinEdge * pixelsPerBinEdge);
        assertEquals(16, numBins);
        assertEquals(16, producedSpatialBins.size());
        int[] expectedIndexes = new int[]{
                2963729, 2963730, 2963731, 2963732,
                2968049, 2968050, 2968051, 2968052,
                2972369, 2972370, 2972371, 2972372,
                2976689, 2976690, 2976691, 2976692,
        };
        for (int i = 0; i < 16; i++) {
            assertEquals(String.format("Problem with bin[%d]", i), expectedIndexes[i], producedSpatialBins.get(i).getIndex());
        }
    }

    private static class MyBinProcessor implements SpatialBinProcessor {
        int numObservationsTotal;
        boolean verbous = false;

        @Override
        public void processSpatialBinSlice(BinningContext ctx, int sliceIndex, List<SpatialBin> sliceBins) {
            if (verbous) {
                // Sort for better readability
                Collections.sort(sliceBins, new BinComparator());
                System.out.println("Slice " + sliceIndex + " =================");
            }
            for (SpatialBin bin : sliceBins) {
                if (verbous) {
                    System.out.println("  writing " + bin.getIndex() + " with " + bin.getNumObs() + " obs.");
                }
                numObservationsTotal += bin.getNumObs();
            }
        }
    }

    private static class BinComparator implements Comparator<SpatialBin> {
        @Override
        public int compare(SpatialBin b1, SpatialBin b2) {
            return b1.getIndex() - b2.getIndex();
        }
    }
}
