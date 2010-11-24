package com.bc.calvalus.b3;

import org.junit.Test;

import static java.lang.Float.NaN;
import static org.junit.Assert.assertEquals;

public class AggregatorTest {
    @Test
    public void testAggregatorAverage() {
        Aggregator agg = new AggregatorAverage(new MyVariableContext("c"), "c");

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

        agg.initSpatial(svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(0.0f, svec.get(1), 0.0f);

        agg.aggregateSpatial(vec(1.5f), svec);
        agg.aggregateSpatial(vec(2.5f), svec);
        agg.aggregateSpatial(vec(0.5f), svec);
        assertEquals(1.5f + 2.5f + 0.5f, svec.get(0), 1e-5f);
        assertEquals(1.5f * 1.5f + 2.5f * 2.5f + 0.5f * 0.5f, svec.get(1), 1e-5f);

        agg.completeSpatial(3, svec);
        assertEquals((1.5f + 2.5f + 0.5f) / 3f, svec.get(0), 1e-5f);
        assertEquals((1.5f * 1.5f + 2.5f * 2.5f + 0.5f * 0.5f) / 3f, svec.get(1), 1e-5f);

        agg.initTemporal(tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(0.0f, tvec.get(1), 0.0f);
        assertEquals(0.0f, tvec.get(2), 0.0f);

        agg.aggregateTemporal(vec(0.3f, 0.09f), 3, tvec);
        agg.aggregateTemporal(vec(0.1f, 0.01f), 2, tvec);
        agg.aggregateTemporal(vec(0.2f, 0.04f), 1, tvec);
        agg.aggregateTemporal(vec(0.1f, 0.01f), 7, tvec);
        assertEquals(0.3f + 0.1f + 0.2f + 0.1f, tvec.get(0), 1e-5f);
        assertEquals(0.09f + 0.01f + 0.04f + 0.01f, tvec.get(1), 1e-5f);
        assertEquals(3f + 2f + 1f + 7f, tvec.get(2), 1e-5f);

        float mean = (0.3f + 0.1f + 0.2f + 0.1f) / (3f + 2f + 1f + 7f);
        float sigma = (float) Math.sqrt((0.09f + 0.01f + 0.04f + 0.01f) / (3f + 2f + 1f + 7f) - mean * mean);
        agg.computeOutput(tvec, out);
        assertEquals(mean, tvec.get(0), 1e-5f);
        assertEquals(sigma, tvec.get(1), 1e-5f);
    }

    @Test
    public void testAggregatorAverageML() {
        Aggregator agg = new AggregatorAverageML(new MyVariableContext("b"), "b");

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
    }

    @Test
    public void tesAggregatorMinMax() {
        Aggregator agg = new AggregatorMinMax(new MyVariableContext("a"), "a");

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

        agg.initSpatial(svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(0.0f, svec.get(1), 0.0f);
        assertEquals(0.0f, svec.get(2), 0.0f);

        agg.aggregateSpatial(vec(1.1f, 0.5f, 7.3f), svec);
        agg.aggregateSpatial(vec(1.5f, 2.5f, 0.1f), svec);
        agg.aggregateSpatial(vec(1.4f, 4.9f, 6.3f), svec);
        assertEquals(1.5f, svec.get(0), 1e-5f);
        assertEquals(2.5f, svec.get(1), 1e-5f);
        assertEquals(0.1f, svec.get(2), 1e-5f);

        agg.completeSpatial(3, svec);
        assertEquals(1.5f, svec.get(0), 1e-5f);
        assertEquals(2.5f, svec.get(1), 1e-5f);
        assertEquals(0.1f, svec.get(2), 1e-5f);

        agg.initTemporal(tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(0.0f, tvec.get(1), 0.0f);
        assertEquals(0.0f, tvec.get(2), 0.0f);

        agg.aggregateTemporal(vec(0.3f, 0.2f, 9.7f), 3, tvec);
        agg.aggregateTemporal(vec(1.1f, 0.1f, 0.3f), 3, tvec);
        agg.aggregateTemporal(vec(4.7f, 0.6f, 7.1f), 3, tvec);
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

}
