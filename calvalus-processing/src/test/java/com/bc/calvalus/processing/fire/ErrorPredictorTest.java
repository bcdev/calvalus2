package com.bc.calvalus.processing.fire;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 */
public class ErrorPredictorTest {

    private ErrorPredictor errorPredictor;

    @Before
    public void setUp() throws Exception {
        errorPredictor = new ErrorPredictor();
    }

    @Test
    public void testPredictError() throws Exception {
        assertArrayEquals(new float[] {0.11469134F, 0.11469134F, 0.11469134F, 0.11469134F}, errorPredictor.predictError(new float[]{0, 0, 0, 0}, new double[]{10, 10, 10, 10}), 1E-5F);
        assertArrayEquals(new float[] {14.060064F, 14.57825F, 37.378433F, 12.764599F}, errorPredictor.predictError(new float[]{10, 12, 100, 5}, new double[]{1000, 1000, 1000, 1000}), 1E-5F);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPredictError_fails() throws Exception {
        errorPredictor.predictError(new float[]{0, 0, 0, 0}, new double[]{10, 10});
    }
}