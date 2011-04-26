package com.bc.calvalus.binning.aggregators;

import com.bc.calvalus.binning.Aggregator;
import com.bc.calvalus.binning.BinContext;
import com.bc.calvalus.binning.MyVariableContext;
import com.bc.calvalus.binning.VectorImpl;
import org.junit.Test;

import java.util.HashMap;

import static java.lang.Float.NaN;
import static java.lang.Math.log;
import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregatorTest {

    BinContext ctx = createCtx();

    @Test
    public void testAggregatorAverageNoWeight() {
        AggregatorAverage agg = new AggregatorAverage(new MyVariableContext("c"), "c", 0.0, null);

        assertEquals("AVG", agg.getName());

        assertEquals(2, agg.getSpatialFeatureNames().length);
        assertEquals("c_sum_x", agg.getSpatialFeatureNames()[0]);
        assertEquals("c_sum_xx", agg.getSpatialFeatureNames()[1]);

        assertEquals(3, agg.getTemporalFeatureNames().length);
        assertEquals("c_sum_x", agg.getTemporalFeatureNames()[0]);
        assertEquals("c_sum_xx", agg.getTemporalFeatureNames()[1]);
        assertEquals("c_sum_w", agg.getTemporalFeatureNames()[2]);

        assertEquals(2, agg.getOutputFeatureNames().length);
        assertEquals("c_mean", agg.getOutputFeatureNames()[0]);
        assertEquals("c_sigma", agg.getOutputFeatureNames()[1]);

        VectorImpl svec = vec(NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(0.0f, svec.get(1), 0.0f);

        agg.aggregateSpatial(ctx, vec(1.5f), svec);
        agg.aggregateSpatial(ctx, vec(2.5f), svec);
        agg.aggregateSpatial(ctx, vec(0.5f), svec);
        float sumX = 1.5f + 2.5f + 0.5f;
        float sumXX = 1.5f * 1.5f + 2.5f * 2.5f + 0.5f * 0.5f;
        assertEquals(sumX, svec.get(0), 1e-5f);
        assertEquals(sumXX, svec.get(1), 1e-5f);

        int numObs = 3;
        agg.completeSpatial(ctx, numObs, svec);
        assertEquals(sumX / numObs, svec.get(0), 1e-5f);
        assertEquals(sumXX / numObs, svec.get(1), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(0.0f, tvec.get(1), 0.0f);
        assertEquals(0.0f, tvec.get(2), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.3f, 0.09f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(0.1f, 0.01f), 2, tvec);
        agg.aggregateTemporal(ctx, vec(0.2f, 0.04f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.1f, 0.01f), 7, tvec);
        assertEquals(0.3f + 0.1f + 0.2f + 0.1f, tvec.get(0), 1e-5f);
        assertEquals(0.09f + 0.01f + 0.04f + 0.01f, tvec.get(1), 1e-5f);
        assertEquals(4f, tvec.get(2), 1e-5f);

        float mean = (0.3f + 0.1f + 0.2f + 0.1f) / 4f;
        float sigma = (float) sqrt((0.09f + 0.01f + 0.04f + 0.01f) / 4f - mean * mean);
        agg.computeOutput(tvec, out);
        assertEquals(mean, out.get(0), 1e-5f);
        assertEquals(sigma, out.get(1), 1e-5f);
    }

    @Test
    public void testAggregatorAverageWeighted() {
        Aggregator agg = new AggregatorAverage(new MyVariableContext("c"), "c", 1.0, null);

        assertEquals("AVG", agg.getName());
        assertTrue(Float.isNaN(agg.getOutputFillValue()));

        assertEquals(2, agg.getSpatialFeatureNames().length);
        assertEquals("c_sum_x", agg.getSpatialFeatureNames()[0]);
        assertEquals("c_sum_xx", agg.getSpatialFeatureNames()[1]);

        assertEquals(3, agg.getTemporalFeatureNames().length);
        assertEquals("c_sum_x", agg.getTemporalFeatureNames()[0]);
        assertEquals("c_sum_xx", agg.getTemporalFeatureNames()[1]);
        assertEquals("c_sum_w", agg.getTemporalFeatureNames()[2]);

        assertEquals(2, agg.getOutputFeatureNames().length);
        assertEquals("c_mean", agg.getOutputFeatureNames()[0]);
        assertEquals("c_sigma", agg.getOutputFeatureNames()[1]);

        VectorImpl svec = vec(NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(0.0f, svec.get(1), 0.0f);

        agg.aggregateSpatial(ctx, vec(1.5f), svec);
        agg.aggregateSpatial(ctx, vec(2.5f), svec);
        agg.aggregateSpatial(ctx, vec(0.5f), svec);
        float sumX = 1.5f + 2.5f + 0.5f;
        float sumXX = 1.5f * 1.5f + 2.5f * 2.5f + 0.5f * 0.5f;
        assertEquals(sumX, svec.get(0), 1e-5f);
        assertEquals(sumXX, svec.get(1), 1e-5f);

        int numObs = 3;
        agg.completeSpatial(ctx, numObs, svec);
        assertEquals(sumX / numObs, svec.get(0), 1e-5f);
        assertEquals(sumXX / numObs, svec.get(1), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(0.0f, tvec.get(1), 0.0f);
        assertEquals(0.0f, tvec.get(2), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.3f, 0.09f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(0.1f, 0.01f), 2, tvec);
        agg.aggregateTemporal(ctx, vec(0.2f, 0.04f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.1f, 0.01f), 7, tvec);
        assertEquals(3 * 0.3f + 2 * 0.1f + 1 * 0.2f + 7 * 0.1f, tvec.get(0), 1e-5f);
        assertEquals(3 * 0.09f + 2 * 0.01f + 1 * 0.04f + 7 * 0.01f, tvec.get(1), 1e-5f);
        assertEquals(3f + 2f + 1f + 7f, tvec.get(2), 1e-5f);

        float mean = (3 * 0.3f + 2 * 0.1f + 1 * 0.2f + 7 * 0.1f) / (3f + 2f + 1f + 7f);
        float sigma = (float) sqrt((3 * 0.09f + 2 * 0.01f + 1 * 0.04f + 7 * 0.01f) / (3f + 2f + 1f + 7f) - mean * mean);
        agg.computeOutput(tvec, out);
        assertEquals(mean, out.get(0), 1e-5f);
        assertEquals(sigma, out.get(1), 1e-5f);
    }

    @Test
    public void testAggregatorAverageML() {
        AggregatorAverageML agg = new AggregatorAverageML(new MyVariableContext("b"), "b", null, null);

        assertEquals("AVG_ML", agg.getName());

        assertTrue(Float.isNaN(agg.getOutputFillValue()));

        assertEquals(2, agg.getSpatialFeatureNames().length);
        assertEquals("b_sum_x", agg.getSpatialFeatureNames()[0]);
        assertEquals("b_sum_xx", agg.getSpatialFeatureNames()[1]);

        assertEquals(3, agg.getTemporalFeatureNames().length);
        assertEquals("b_sum_x", agg.getTemporalFeatureNames()[0]);
        assertEquals("b_sum_xx", agg.getTemporalFeatureNames()[1]);
        assertEquals("b_sum_w", agg.getTemporalFeatureNames()[2]);

        assertEquals(4, agg.getOutputFeatureNames().length);
        assertEquals("b_mean", agg.getOutputFeatureNames()[0]);
        assertEquals("b_sigma", agg.getOutputFeatureNames()[1]);
        assertEquals("b_median", agg.getOutputFeatureNames()[2]);
        assertEquals("b_mode", agg.getOutputFeatureNames()[3]);

        VectorImpl svec = vec(NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN, NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(0.0f, svec.get(1), 0.0f);

        agg.aggregateSpatial(ctx, vec(1.5f), svec);
        agg.aggregateSpatial(ctx, vec(2.5f), svec);
        agg.aggregateSpatial(ctx, vec(0.5f), svec);
        assertEquals(log(1.5f) + log(2.5f) + log(0.5f), svec.get(0), 1e-5);
        assertEquals(log(1.5f) * log(1.5f) + log(2.5f) * log(2.5f) + log(0.5f) * log(0.5f), svec.get(1), 1e-5f);

        agg.completeSpatial(ctx, 3, svec);
        assertEquals((log(1.5f) + log(2.5f) + log(0.5f)) / sqrt(3f), svec.get(0), 1e-5f);
        assertEquals((log(1.5f) * log(1.5f) + log(2.5f) * log(2.5f) + log(0.5f) * log(0.5f)) / sqrt(3f), svec.get(1), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(0.0f, tvec.get(1), 0.0f);
        assertEquals(0.0f, tvec.get(2), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.3f, 0.09f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(0.1f, 0.01f), 2, tvec);
        agg.aggregateTemporal(ctx, vec(0.2f, 0.04f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.1f, 0.01f), 7, tvec);
        assertEquals(0.3f + 0.1f + 0.2f + 0.1f, tvec.get(0), 1e-5);
        assertEquals(0.09f + 0.01f + 0.04f + 0.01f, tvec.get(1), 1e-5);
        assertEquals(sqrt(3f) + sqrt(2f) + sqrt(1f) + sqrt(7f), tvec.get(2), 1e-5);

        /*
        todo - add asserts for output values
        float mean = ...;
        float sigma = ...;
        float median = ...;
        float mode = ...;
        agg.computeOutput(tvec, out);
        assertEquals(mean, out.get(0), 1e-5f);
        assertEquals(sigma, out.get(1), 1e-5f);
        assertEquals(median, out.get(2), 1e-5f);
        assertEquals(mode, out.get(3), 1e-5f);
         */
    }

    @Test
    public void testAggregatorPercentile() {
        AggregatorPercentile agg = new AggregatorPercentile(new MyVariableContext("c"), "c", null, null);

        assertEquals("c_sum_x", agg.getSpatialFeatureNames()[0]);
        assertEquals("c_P90", agg.getTemporalFeatureNames()[0]);
        assertEquals("c_P90", agg.getOutputFeatureNames()[0]);
        assertTrue(Float.isNaN(agg.getOutputFillValue()));

        agg = new AggregatorPercentile(new MyVariableContext("c"), "c", 70, 0.42F);

        assertEquals("PERCENTILE", agg.getName());

        assertEquals(1, agg.getSpatialFeatureNames().length);
        assertEquals("c_sum_x", agg.getSpatialFeatureNames()[0]);

        assertEquals(1, agg.getTemporalFeatureNames().length);
        assertEquals("c_P70", agg.getTemporalFeatureNames()[0]);

        assertEquals(1, agg.getOutputFeatureNames().length);
        assertEquals("c_P70", agg.getOutputFeatureNames()[0]);

        assertEquals(0.42F, agg.getOutputFillValue(), 1e-5F);

        VectorImpl svec = vec(NaN);
        VectorImpl tvec = vec(NaN);
        VectorImpl out = vec(NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(0.0f, svec.get(0), 0.0f);

        agg.aggregateSpatial(ctx, vec(1.5f), svec);
        agg.aggregateSpatial(ctx, vec(2.5f), svec);
        agg.aggregateSpatial(ctx, vec(0.5f), svec);
        float sumX = 1.5f + 2.5f + 0.5f;
        assertEquals(sumX, svec.get(0), 1e-5f);

        int numObs = 3;
        agg.completeSpatial(ctx, numObs, svec);
        assertEquals(sumX / numObs, svec.get(0), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.1f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.2f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.3f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.4f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.5f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.6f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.7f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.8f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.9f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(1.0f), 1, tvec);
        assertEquals(0.0f, tvec.get(0), 1e-5f);

        agg.completeTemporal(ctx, 10, tvec);
        assertEquals(0.77f, tvec.get(0), 1e-5f);

        agg.computeOutput(tvec, out);
        assertEquals(0.77f, out.get(0), 1e-5f);
    }

    @Test
    public void tesAggregatorMinMax() {
        AggregatorMinMax agg = new AggregatorMinMax(new MyVariableContext("a"), "a", null);

        assertEquals("MIN_MAX", agg.getName());

        assertEquals(2, agg.getSpatialFeatureNames().length);
        assertEquals("a_min", agg.getSpatialFeatureNames()[0]);
        assertEquals("a_max", agg.getSpatialFeatureNames()[1]);

        assertEquals(2, agg.getTemporalFeatureNames().length);
        assertEquals("a_min", agg.getTemporalFeatureNames()[0]);
        assertEquals("a_max", agg.getTemporalFeatureNames()[1]);

        assertEquals(2, agg.getOutputFeatureNames().length);
        assertEquals("a_min", agg.getOutputFeatureNames()[0]);
        assertEquals("a_max", agg.getOutputFeatureNames()[1]);

        VectorImpl svec = vec(NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN);
        VectorImpl out = vec(NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(Float.POSITIVE_INFINITY, svec.get(0), 0.0f);
        assertEquals(Float.NEGATIVE_INFINITY, svec.get(1), 0.0f);

        agg.aggregateSpatial(ctx, vec(7.3f), svec);
        agg.aggregateSpatial(ctx, vec(5.5f), svec);
        agg.aggregateSpatial(ctx, vec(-0.1f), svec);
        agg.aggregateSpatial(ctx, vec(2.0f), svec);
        assertEquals(-0.1f, svec.get(0), 1e-5f);
        assertEquals(7.3f, svec.get(1), 1e-5f);

        agg.completeSpatial(ctx, 3, svec);
        assertEquals(-0.1f, svec.get(0), 1e-5f);
        assertEquals(7.3f, svec.get(1), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(Float.POSITIVE_INFINITY, tvec.get(0), 0.0f);
        assertEquals(Float.NEGATIVE_INFINITY, tvec.get(1), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.9f, 1.0f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(0.1f, 5.1f), 5, tvec);
        agg.aggregateTemporal(ctx, vec(0.6f, 2.0f), 9, tvec);
        agg.aggregateTemporal(ctx, vec(0.2f, 1.5f), 2, tvec);
        assertEquals(0.1f, tvec.get(0), 1e-5f);
        assertEquals(5.1f, tvec.get(1), 1e-5f);

        agg.computeOutput(tvec, out);
        assertEquals(0.1f, tvec.get(0), 1e-5f);
        assertEquals(5.1f, tvec.get(1), 1e-5f);
    }

    @Test
    public void testAggregatorOnMaxSet() {
        AggregatorOnMaxSet agg = new AggregatorOnMaxSet(new MyVariableContext("a", "b", "c"), "c", "a", "b");

        assertEquals("ON_MAX_SET", agg.getName());

        assertEquals(3, agg.getSpatialFeatureNames().length);
        assertEquals("c_max", agg.getSpatialFeatureNames()[0]);
        assertEquals("a", agg.getSpatialFeatureNames()[1]);
        assertEquals("b", agg.getSpatialFeatureNames()[2]);

        assertEquals(3, agg.getTemporalFeatureNames().length);
        assertEquals("c_max", agg.getTemporalFeatureNames()[0]);
        assertEquals("a", agg.getTemporalFeatureNames()[1]);
        assertEquals("b", agg.getTemporalFeatureNames()[2]);

        assertEquals(3, agg.getOutputFeatureNames().length);
        assertEquals("c_max", agg.getOutputFeatureNames()[0]);
        assertEquals("a", agg.getOutputFeatureNames()[1]);
        assertEquals("b", agg.getOutputFeatureNames()[2]);

        VectorImpl svec = vec(NaN, NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN, NaN);

        agg.initSpatial(ctx, svec);
        assertEquals(Float.NEGATIVE_INFINITY, svec.get(0), 0.0f);
        assertEquals(NaN, svec.get(1), 0.0f);
        assertEquals(NaN, svec.get(2), 0.0f);

        agg.aggregateSpatial(ctx, vec(7.3f, 0.5f, 1.1f), svec);
        agg.aggregateSpatial(ctx, vec(0.1f, 2.5f, 1.5f), svec);
        agg.aggregateSpatial(ctx, vec(5.5f, 4.9f, 1.4f), svec);
        assertEquals(1.5f, svec.get(0), 1e-5f);
        assertEquals(0.1f, svec.get(1), 1e-5f);
        assertEquals(2.5f, svec.get(2), 1e-5f);

        agg.completeSpatial(ctx, 3, svec);
        assertEquals(1.5f, svec.get(0), 1e-5f);
        assertEquals(0.1f, svec.get(1), 1e-5f);
        assertEquals(2.5f, svec.get(2), 1e-5f);

        agg.initTemporal(ctx, tvec);
        assertEquals(Float.NEGATIVE_INFINITY, tvec.get(0), 0.0f);
        assertEquals(NaN, tvec.get(1), 0.0f);
        assertEquals(NaN, tvec.get(2), 0.0f);

        agg.aggregateTemporal(ctx, vec(0.3f, 0.2f, 9.7f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(1.1f, 0.1f, 0.3f), 3, tvec);
        agg.aggregateTemporal(ctx, vec(4.7f, 0.6f, 7.1f), 3, tvec);
        assertEquals(4.7f, tvec.get(0), 1e-5f);
        assertEquals(0.6f, tvec.get(1), 1e-5f);
        assertEquals(7.1f, tvec.get(2), 1e-5f);

        agg.computeOutput(tvec, out);
        assertEquals(4.7f, out.get(0), 1e-5f);
        assertEquals(0.6f, out.get(1), 1e-5f);
        assertEquals(7.1f, out.get(2), 1e-5f);
    }

    @Test
    public void testSuperSampling() {
        Aggregator agg = new AggregatorAverage(new MyVariableContext("c"), "c", null, null);
        VectorImpl svec = vec(NaN, NaN);
        VectorImpl tvec = vec(NaN, NaN, NaN);
        VectorImpl out = vec(NaN, NaN);

        agg.initSpatial(ctx, svec);
        agg.aggregateSpatial(ctx, vec(1.5f), svec);
        agg.aggregateSpatial(ctx, vec(2.5f), svec);
        agg.aggregateSpatial(ctx, vec(0.5f), svec);

        agg.aggregateSpatial(ctx, vec(1.5f), svec);
        agg.aggregateSpatial(ctx, vec(2.5f), svec);
        agg.aggregateSpatial(ctx, vec(0.5f), svec);

        agg.aggregateSpatial(ctx, vec(1.5f), svec);
        agg.aggregateSpatial(ctx, vec(2.5f), svec);
        agg.aggregateSpatial(ctx, vec(0.5f), svec);

        float sumX = (1.5f + 2.5f + 0.5f) * 3;
        float sumXX = (1.5f * 1.5f + 2.5f * 2.5f + 0.5f * 0.5f) * 3;
        assertEquals(sumX, svec.get(0), 1e-5f);
        assertEquals(sumXX, svec.get(1), 1e-5f);

        int numObs = 9;
        agg.completeSpatial(ctx, numObs, svec);
        assertEquals(sumX / numObs, svec.get(0), 1e-5f);
        assertEquals(sumXX / numObs, svec.get(1), 1e-5f);
    }

    private VectorImpl vec(float... values) {
        return new VectorImpl(values);
    }

    static BinContext createCtx() {
        return new BinContext() {
            private HashMap map = new HashMap();

            @Override
            public long getIndex() {
                return 0;
            }

            @Override
            public <T> T get(String name) {
                return (T) map.get(name);
            }

            @Override
            public void put(String name, Object value) {
                map.put(name, value);
            }
        };
    }
}
