package com.bc.calvalus.processing.fire.format.grid;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 */
public class ErrorPredictorTest {

    private ErrorPredictor errorPredictor;

    @Before
    public void setUp() throws Exception {
        errorPredictor = new ErrorPredictor();
    }

    @After
    public void tearDown() throws Exception {
        errorPredictor.dispose();

    }

    @Test
    public void testPredictError() throws Exception {
        assertArrayEquals(new float[]{0.0F, 0.0F, 0.0F, 0.0F}, errorPredictor.predictError(new float[]{0, 0, 0, 0}, new double[]{10, 10, 10, 10}), 1E-5F);
        assertArrayEquals(new float[]{3.267012F, 3.9204144F, 32.67012F, 1.633506F}, errorPredictor.predictError(new float[]{10, 12, 100, 5}, new double[]{1000, 1000, 1000, 1000}), 1E-5F);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPredictError_fails() throws Exception {
        errorPredictor.predictError(new float[]{0, 0, 0, 0}, new double[]{10, 10});
    }
}