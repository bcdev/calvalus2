package com.bc.calvalus.processing.fire.format.pixel;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class PixelReducerTest {

    @Test
    public void testGetTargetValues() throws Exception {
        short[] values = new short[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        short[] data = PixelReducer.getTargetValues(0, 3, 0, 3, values);
        assertArrayEquals(new short[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = PixelReducer.getTargetValues(1, 3, 0, 3, values);
        assertArrayEquals(new short[]{1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 14, 15}, data);

        data = PixelReducer.getTargetValues(0, 3, 2, 3, values);
        assertArrayEquals(new short[]{8, 9, 10, 11, 12, 13, 14, 15}, data);

        data = PixelReducer.getTargetValues(0, 2, 0, 3, values);
        assertArrayEquals(new short[]{0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14}, data);

        data = PixelReducer.getTargetValues(1, 3, 1, 3, values);
        assertArrayEquals(new short[]{5, 6, 7, 9, 10, 11, 13, 14, 15}, data);
    }

}
