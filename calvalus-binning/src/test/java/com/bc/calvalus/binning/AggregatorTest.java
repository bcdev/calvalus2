package com.bc.calvalus.binning;

import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import static java.lang.Float.*;
import static java.lang.Math.*;
import static org.junit.Assert.*;

public class AggregatorTest {

     BinContext ctx = createCtx();

    @Test
    public void testWeightFN() {
        AggregatorAverage.WeightFn f;

        f = AggregatorAverage.getWeightFn(0.0);
        assertNotNull(f);
        assertEquals(1.0f, f.eval(0), 1e-10f);
        assertEquals(1.0f, f.eval(1), 1e-10f);
        assertEquals(1.0f, f.eval(2), 1e-10f);
        assertEquals(1.0f, f.eval(4), 1e-10f);

        f = AggregatorAverage.getWeightFn(1.0);
        assertNotNull(f);
        assertEquals(0.0f, f.eval(0), 1e-10f);
        assertEquals(1.0f, f.eval(1), 1e-10f);
        assertEquals(2.0f, f.eval(2), 1e-10f);
        assertEquals(4.0f, f.eval(4), 1e-10f);

        f = AggregatorAverage.getWeightFn(0.5);
        assertNotNull(f);
        assertEquals(0.0f, f.eval(0), 1e-10f);
        assertEquals(1.0f, f.eval(1), 1e-10f);
        assertEquals((float)Math.sqrt(2), f.eval(2), 1e-10f);
        assertEquals(2.0f, f.eval(4), 1e-10f);

        f = AggregatorAverage.getWeightFn(0.42);
        assertNotNull(f);
        assertEquals((float)Math.pow(0, 0.42), f.eval(0), 1e-10f);
        assertEquals((float)Math.pow(1, 0.42), f.eval(1), 1e-10f);
        assertEquals((float)Math.pow(2, 0.42), f.eval(2), 1e-10f);
        assertEquals((float)Math.pow(4, 0.42), f.eval(4), 1e-10f);
    }

    @Test
    public void testAggregatorAverageNoWeight() {
        Aggregator agg = new AggregatorAverage(new MyVariableContext("c"), "c", 0.0, null);

        assertEquals("AVG", agg.getName());

        assertEquals(2, agg.getSpatialPropertyCount());
        assertEquals("c_sum_x", agg.getSpatialPropertyName(0));
        assertEquals("c_sum_xx", agg.getSpatialPropertyName(1));

        assertEquals(3, agg.getTemporalPropertyCount());
        assertEquals("c_sum_x", agg.getTemporalPropertyName(0));
        assertEquals("c_sum_xx", agg.getTemporalPropertyName(1));
        assertEquals("c_sum_w", agg.getTemporalPropertyName(2));

        assertEquals(2, agg.getOutputPropertyCount());
        assertEquals("c_mean", agg.getOutputPropertyName(0));
        assertEquals("c_sigma", agg.getOutputPropertyName(1));

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

        assertEquals(2, agg.getSpatialPropertyCount());
        assertEquals("c_sum_x", agg.getSpatialPropertyName(0));
        assertEquals("c_sum_xx", agg.getSpatialPropertyName(1));

        assertEquals(3, agg.getTemporalPropertyCount());
        assertEquals("c_sum_x", agg.getTemporalPropertyName(0));
        assertEquals("c_sum_xx", agg.getTemporalPropertyName(1));
        assertEquals("c_sum_w", agg.getTemporalPropertyName(2));

        assertEquals(2, agg.getOutputPropertyCount());
        assertEquals("c_mean", agg.getOutputPropertyName(0));
        assertEquals("c_sigma", agg.getOutputPropertyName(1));

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
        assertEquals(3 *0.09f + 2 * 0.01f + 1 *0.04f + 7 * 0.01f, tvec.get(1), 1e-5f);
        assertEquals(3f + 2f + 1f + 7f, tvec.get(2), 1e-5f);

        float mean = (3 * 0.3f + 2 * 0.1f + 1 * 0.2f + 7 * 0.1f) / (3f + 2f + 1f + 7f);
        float sigma = (float) sqrt((3 *0.09f + 2 * 0.01f + 1 *0.04f + 7 * 0.01f) / (3f + 2f + 1f + 7f) - mean * mean);
        agg.computeOutput(tvec, out);
        assertEquals(mean, out.get(0), 1e-5f);
        assertEquals(sigma, out.get(1), 1e-5f);
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

    @Test
    public void testAggregatorAverageML() {
        Aggregator agg = new AggregatorAverageML(new MyVariableContext("b"), "b", null, null);

        assertEquals("AVG_ML", agg.getName());

        assertEquals(2, agg.getSpatialPropertyCount());
        assertEquals("b_sum_x", agg.getSpatialPropertyName(0));
        assertEquals("b_sum_xx", agg.getSpatialPropertyName(1));

        assertEquals(3, agg.getTemporalPropertyCount());
        assertEquals("b_sum_x", agg.getTemporalPropertyName(0));
        assertEquals("b_sum_xx", agg.getTemporalPropertyName(1));
        assertEquals("b_sum_w", agg.getTemporalPropertyName(2));

        assertEquals(4, agg.getOutputPropertyCount());
        assertEquals("b_mean", agg.getOutputPropertyName(0));
        assertEquals("b_sigma", agg.getOutputPropertyName(1));
        assertEquals("b_median", agg.getOutputPropertyName(2));
        assertEquals("b_mode", agg.getOutputPropertyName(3));

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
        Aggregator agg = new AggregatorPercentile(new MyVariableContext("c"), "c", 70, null);

        assertEquals("PERCENTILE", agg.getName());

        assertEquals(1, agg.getSpatialPropertyCount());
        assertEquals("c_sum_x", agg.getSpatialPropertyName(0));

        assertEquals(1, agg.getTemporalPropertyCount());
        assertEquals("c_P70", agg.getTemporalPropertyName(0));

        assertEquals(1, agg.getOutputPropertyCount());
        assertEquals("c_P70", agg.getOutputPropertyName(0));

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
        agg.aggregateTemporal(ctx, vec(0.1f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.2f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.2f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.3f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.3f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.4f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.4f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.5f), 1, tvec);
        agg.aggregateTemporal(ctx, vec(0.5f), 1, tvec);
        assertEquals(0.0f, tvec.get(0), 1e-5f);

        agg.completeTemporal(ctx, 10, tvec);
        assertEquals(0.47f, tvec.get(0), 1e-5f);

        agg.computeOutput(tvec, out);
        assertEquals(0.47f, out.get(0), 1e-5f);
    }

    @Test
    public void tesAggregatorMinMax() {
        Aggregator agg = new AggregatorMinMax(new MyVariableContext("a"), "a", null);

        assertEquals("MIN_MAX", agg.getName());

        assertEquals(2, agg.getSpatialPropertyCount());
        assertEquals("a_min", agg.getSpatialPropertyName(0));
        assertEquals("a_max", agg.getSpatialPropertyName(1));

        assertEquals(2, agg.getTemporalPropertyCount());
        assertEquals("a_min", agg.getTemporalPropertyName(0));
        assertEquals("a_max", agg.getTemporalPropertyName(1));

        assertEquals(2, agg.getOutputPropertyCount());
        assertEquals("a_min", agg.getOutputPropertyName(0));
        assertEquals("a_max", agg.getOutputPropertyName(1));

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
        Aggregator agg = new AggregatorOnMaxSet(new MyVariableContext("a", "b", "c"), "c", "a", "b");

        assertEquals("ON_MAX_SET", agg.getName());

        assertEquals(3, agg.getSpatialPropertyCount());
        assertEquals("c_max", agg.getSpatialPropertyName(0));
        assertEquals("a", agg.getSpatialPropertyName(1));
        assertEquals("b", agg.getSpatialPropertyName(2));

        assertEquals(3, agg.getTemporalPropertyCount());
        assertEquals("c_max", agg.getTemporalPropertyName(0));
        assertEquals("a", agg.getTemporalPropertyName(1));
        assertEquals("b", agg.getTemporalPropertyName(2));

        assertEquals(3, agg.getOutputPropertyCount());
        assertEquals("c_max", agg.getOutputPropertyName(0));
        assertEquals("a", agg.getOutputPropertyName(1));
        assertEquals("b", agg.getOutputPropertyName(2));

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

    private VectorImpl vec(float... values) {
        return new VectorImpl(values);
    }

    static BinContext createCtx() {
        return new BinContext() {
            private  HashMap map = new HashMap();

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
