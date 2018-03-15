package com.bc.calvalus.processing.fire.format.pixel.s2;

import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.support.ObservationImpl;
import org.esa.snap.binning.support.VectorImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S2JDAggregatorTest {

    public static final int NUM_OBS = -1;

    @Test
    public void testAggregate_1() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 20F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998F, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0F, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(20F, temporalVector.get(0), 1E-7);
        assertEquals(0.5F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_2() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998F, 998F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999F, 999F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 997F, 997F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(997F, temporalVector.get(0), 1E-7);
        assertEquals(0, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_3() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 10F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 20F, 0.8F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999F, 999F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 11F, 0.6F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(20F, temporalVector.get(0), 1E-7);
        assertEquals(0.8, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_4() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 5F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 10F, 0.8F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 100F, 0.3F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998F, 998F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999F, 999F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 45F, 0.2F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998F, 998F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(10F, temporalVector.get(0), 1E-7);
        assertEquals(0.8, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_cloudsFirst() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998F, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 20F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0F, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(20F, temporalVector.get(0), 1E-7);
        assertEquals(0.5F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_temporalCloudsAreOverwritten() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        SpatialBin spatialCtx2 = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl spatialVector2 = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.initSpatial(spatialCtx2, spatialVector2);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998F, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 20F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0F, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx2, new ObservationImpl(0, 0, 0, 998F, 0), spatialVector2);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector2);

        assertEquals(20F, spatialVector.get(0), 1E-7);
        assertEquals(0.5F, spatialVector.get(1), 1E-7);
        assertEquals(998F, spatialVector2.get(0), 1E-7);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector2, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(20F, temporalVector.get(0), 1E-7);
        assertEquals(0.5F, temporalVector.get(1), 1E-7);

    }

    @Test
    public void testAggregate_NoValidObservations() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{32, 60});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 10F, 0.2F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(0F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_waterIsPreserved() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 997, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(997F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_onlyInvalidObs() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(999F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_noObsAreOverwritten() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 10F, 0.3F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(10F, temporalVector.get(0), 1E-7);
        assertEquals(0.3F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_noObsAreOverwrittenWithZero() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0, 0.0F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(0F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_everythingCloudy() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998, 998), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998, 998), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998, 998), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(998F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_everythingCloudy2() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998, 998), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 998, 998), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 999, 999), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(998F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_supplementData1() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -100F, 0F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 20F, 0.5F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(20F, temporalVector.get(0), 1E-7);
        assertEquals(0.5F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_supplementData2() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -100F, 0F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(998F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_supplementData3() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -100F, 0F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -101F, 0F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(998F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_supplementData4() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 20F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -100F, 0F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(20F, temporalVector.get(0), 1E-7);
        assertEquals(0.5F, temporalVector.get(1), 1E-7);
    }


    @Test
    public void testAggregate_supplementData5() throws Exception {
        S2JDAggregator aggregator = new S2JDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -101F, 0F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(0F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }
}