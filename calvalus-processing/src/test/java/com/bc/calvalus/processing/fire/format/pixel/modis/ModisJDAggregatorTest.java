package com.bc.calvalus.processing.fire.format.pixel.modis;

import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.support.ObservationImpl;
import org.esa.snap.binning.support.VectorImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ModisJDAggregatorTest {

    public static final int NUM_OBS = -1;

    @Test
    public void testAggregate_1() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 20F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, 0), spatialVector);
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
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1F, -1F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1F, -1F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -2F, -2F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(-2F, temporalVector.get(0), 1E-7);
        assertEquals(0, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_3() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 10F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 20F, 0.8F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1F, -1F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 11F, 0.6F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(10F, temporalVector.get(0), 1E-7);
        assertEquals(0.5, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_4() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 5F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 10F, 0.8F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 100F, 0.3F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1F, -1F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1F, -1F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 45F, 0.2F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1F, -1F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(5F, temporalVector.get(0), 1E-7);
        assertEquals(0.5, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_CL_is_set_if_JD_is_zero() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0F, 0.5F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(0F, temporalVector.get(0), 1E-7);
        assertEquals(0.5F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_CL_is_set_if_JD_is_zero_2() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -2F, 0.5F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0F, 0.3F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(0F, temporalVector.get(0), 1E-7);
        assertEquals(0.3F, temporalVector.get(1), 1E-7);
    }


    @Test
    public void testAggregate_NoValidObservations() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{32, 60});

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

        assertEquals(-1F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_onlyInvalidObs() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(-1F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_noObsAreOverwritten() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -2, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 10F, 0.3F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -2, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(10F, temporalVector.get(0), 1E-7);
        assertEquals(0.3F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_noObsAreOverwrittenWithZero() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -2, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0, 0.0F), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -2, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(0F, temporalVector.get(0), 1E-7);
        assertEquals(1F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_everythingCloudy() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, -1), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, -1), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, -1), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(-1F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_everythingCloudy2() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, -1, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(-1F, temporalVector.get(0), 1E-7);
        assertEquals(0F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_CL_is_filled() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0, 0), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(0F, temporalVector.get(0), 1E-7);
        assertEquals(1F, temporalVector.get(1), 1E-7);
    }

    @Test
    public void testAggregate_CL_is_filled_2() throws Exception {
        Aggregator aggregator = new ModisJDAggregator(null, null, null, null, new int[]{1, 30});

        SpatialBin spatialCtx = new SpatialBin();
        TemporalBin temporalCtx = new TemporalBin();

        VectorImpl spatialVector = new VectorImpl(new float[2]);
        VectorImpl temporalVector = new VectorImpl(new float[2]);

        aggregator.initSpatial(spatialCtx, spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 0, 0), spatialVector);
        aggregator.aggregateSpatial(spatialCtx, new ObservationImpl(0, 0, 0, 1, 0.3F), spatialVector);
        aggregator.completeSpatial(spatialCtx, NUM_OBS, spatialVector);

        aggregator.initTemporal(temporalCtx, temporalVector);
        aggregator.aggregateTemporal(temporalCtx, spatialVector, 0, temporalVector);
        aggregator.completeTemporal(temporalCtx, 0, temporalVector);

        assertEquals(1F, temporalVector.get(0), 1E-7);
        assertEquals(0.3F, temporalVector.get(1), 1E-7);
    }

}