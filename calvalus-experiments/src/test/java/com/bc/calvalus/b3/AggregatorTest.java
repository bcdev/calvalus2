package com.bc.calvalus.b3;

import org.junit.Test;

import static org.junit.Assert.*;

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
    }

}
