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
        assertArrayEquals(new float[]{0, 0, 0, 0}, errorPredictor.predictError(new float[]{0, 0, 0, 0}, new double[]{10, 10, 10, 10}), 1E-5f);
        assertArrayEquals(new float[]{3, 3, 32, 1}, errorPredictor.predictError(new float[]{10, 12, 100, 5}, new double[]{1000, 1000, 1000, 1000}), 1E-5f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPredictError_fails() throws Exception {
        errorPredictor.predictError(new float[]{0, 0, 0, 0}, new double[]{10, 10});
    }
}