package com.bc.calvalus.processing.fire.format.pixel.s2;

import org.esa.snap.binning.WritableVector;
import org.esa.snap.binning.support.VectorImpl;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class S2JDAggregatorTest {

    private static final float EPS = 1e-5f;

    @Test
    public void aggregate() {
        S2JDAggregator agg = new S2JDAggregator("...",
                                                new String[] {"MJD", "JD", "CL"},
                                                new String[] {"MJD", "JD", "CL"},
                                                new String[] {"JD", "CL"},
                                                new int[] {31 + 28 + 1, 31 + 28 + 31},
                                                1059.0f,
                                                1090.0f);
        // start with NO_DATA
        WritableVector state = new VectorImpl(new float[] { -1.0f, 999.0f, 0.0f });

        // observe CLOUD
        agg.aggregate(1071.0f, 998.0f, 0.0f, state);
        assertEquals("mjd", -1.0f, state.get(0), EPS);
        assertEquals("jd", 998.0f, state.get(1), EPS);
        assertEquals("cl", 0.0f, state.get(2), EPS);

        // observe CLEAR_LAND
        agg.aggregate(1070.0f, 0.0f, 0.0f, state);
        assertEquals("mjd", -1.0f, state.get(0), EPS);
        assertEquals("jd", 0.0f, state.get(1), EPS);
        assertEquals("cl", 0.0f, state.get(2), EPS);

        // observe burned
        agg.aggregate(1072.0f, 72.0f, 0.6f, state);
        assertEquals("mjd", 1072.0f, state.get(0), EPS);
        assertEquals("jd", 72.0f, state.get(1), EPS);
        assertEquals("cl", 0.6f, state.get(2), EPS);

        // observe burned a few days earlier
        agg.aggregate(1065.0f, 65.0f, 0.4f, state);
        assertEquals("mjd", 1065.0f, state.get(0), EPS);
        assertEquals("jd", 65.0f, state.get(1), EPS);
        assertEquals("cl", 0.4f, state.get(2), EPS);

        // observe burned in previous month
        agg.aggregate(1040.0f, 40.0f, 0.2f, state);
        assertEquals("mjd", 1040.0f, state.get(0), EPS);
        assertEquals("jd", 0.0f, state.get(1), EPS);
        assertEquals("cl", 0.0f, state.get(2), EPS);

        // observe WATER
        agg.aggregate(1073.0f, 997.0f, 0.0f, state);
        assertEquals("mjd", 1040.0f, state.get(0), EPS);
        assertEquals("jd", 997.0f, state.get(1), EPS);
        assertEquals("cl", 0.0f, state.get(2), EPS);
    }

    @Test
    public void aggregate2() {
        S2JDAggregator agg = new S2JDAggregator("...",
                                                new String[]{"MJD", "JD", "CL"},
                                                new String[]{"MJD", "JD", "CL"},
                                                new String[]{"JD", "CL"},
                                                new int[]{31 + 28 + 1, 31 + 28 + 31},
                                                6999.0f,
                                                7029.0f);
        // start with NO_DATA
        WritableVector state = new VectorImpl(new float[]{6999.5f, 60.0f, 0.9f});

        // observe CLOUD
        agg.aggregate(6994.5f, 55.0f, 0.8f, state);
        assertEquals("mjd", 6994.5f, state.get(0), EPS);
        assertEquals("jd", 0.0f, state.get(1), EPS);
        assertEquals("cl", 0.0f, state.get(2), EPS);
    }
}