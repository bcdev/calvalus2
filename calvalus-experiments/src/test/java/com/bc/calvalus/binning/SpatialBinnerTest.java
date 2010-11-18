package com.bc.calvalus.binning;

import org.junit.Test;

import java.awt.geom.AffineTransform;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.lang.Math.*;
import static org.junit.Assert.*;


public class SpatialBinnerTest {
    @Test
    public void testThatObservationsAreAggregated() {
        MyBinFactory factory = new MyBinFactory();
        MyBinStore store = new MyBinStore();
        BinConsumer<MyBin> consumer = new TemporalBinner<MyBin>(store);
        SpatialBinner<MyObservation, MyBin> producer = new SpatialBinner<MyObservation, MyBin>(new MyBinningGrid(), factory, consumer, 2);

        producer.processSlice(new MyObservation(0, 1, 1.1),
                              new MyObservation(0, 1, 1.2),
                              new MyObservation(0, 2, 1.3),
                              new MyObservation(0, 2, 1.4));

        producer.processSlice(new MyObservation(0, 1, 2.1),
                              new MyObservation(0, 2, 2.2),
                              new MyObservation(0, 2, 2.3),
                              new MyObservation(0, 2, 2.4),
                              new MyObservation(0, 3, 2.5));

        assertEquals(3, store.binMap.size());

        MyBin actualB1 = store.binMap.get(1);
        assertNotNull(actualB1);
        assertEquals(3, actualB1.numObservations);
        assertEquals(sqrt(3), actualB1.weight, 1e-5);
        assertEquals((log(1.1) + log(1.2) + log(2.1)) / sqrt(3), actualB1.sumX, 1e-5);

        MyBin actualB2 = store.binMap.get(2);
        assertNotNull(actualB2);
        assertEquals(5, actualB2.numObservations);
        assertEquals(sqrt(5), actualB2.weight, 1e-5);
        assertEquals((log(1.3) + log(1.4) + log(2.2) + log(2.3) + log(2.4)) / sqrt(5), actualB2.sumX, 1e-5);

        MyBin actualB3 = store.binMap.get(3);
        assertNotNull(actualB3);
        assertEquals(1, actualB3.numObservations);
        assertEquals(1.0, actualB3.weight, 1e-10);
        assertEquals(log(2.5), actualB3.sumX, 1e-5);
    }

    @Test
    public void testThatCellsAreDeterminedCorrectly() {
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
        double deltaLon = w * pixelEdgeSize;
        double deltaLat = h * pixelEdgeSize;
        MyObservation[][] pixelSlices = new MyObservation[h][w];
        AffineTransform at = AffineTransform.getRotateInstance(Math.toRadians(rotAngle));
        for (int j = 0; j < h; j++) {
            for (int i = 0; i < w; i++) {
                double[] srcPts = new double[]{
                        -deltaLat * 0.5 + (deltaLat * j) / h,
                        -deltaLon * 0.5 + (deltaLon * i) / w
                };
                double[] dstPts = new double[2];
                at.transform(srcPts, 0, dstPts, 0, 1);
                pixelSlices[j][i] = new MyObservation(dstPts[0], dstPts[1]);
            }
        }

        MyBinFactory factory = new MyBinFactory();
        MyBinConsumer consumer = new MyBinConsumer();
        SpatialBinner<MyObservation, MyBin> producer = new SpatialBinner<MyObservation, MyBin>(grid, factory, consumer, h);

        for (MyObservation[] pixelSlice : pixelSlices) {
            producer.processSlice(pixelSlice);
        }

        assertEquals(w * h, consumer.numObservationsTotal);
        assertEquals(w * h / (pixelsPerBinEdge * pixelsPerBinEdge), factory.numBins);
    }

    private static class MyBinConsumer implements BinConsumer<MyBin> {
        int numObservationsTotal;

        @Override
        public void consumeSlice(int sliceIndex, List<MyBin> sliceBins) {
            // Sort for better readability
            Collections.sort(sliceBins, new BinComparator<MyBin>());
            System.out.println("Slice " + sliceIndex + " =================");
            for (MyBin bin : sliceBins) {
                System.out.println("  writing " + bin.getIndex() + " with " + bin.getNumObservations() + " obs.");
                numObservationsTotal += bin.getNumObservations();
            }
        }
    }

    private static class MyBinningGrid implements BinningGrid {
        @Override
        public int getBinIndex(double lat, double lon) {
            return (int) lon;
        }
    }

    private static class MyBinFactory implements BinFactory<MyBin> {
        int numBins;

        @Override
        public MyBin createBin(int binIndex) {
            numBins++;
            return new MyBin(binIndex);
        }
    }

    private static class BinComparator<BIN extends Bin> implements Comparator<BIN> {
        @Override
        public int compare(BIN b1, BIN b2) {
            return b1.getIndex() - b2.getIndex();
        }
    }
}
