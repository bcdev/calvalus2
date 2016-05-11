package com.bc.calvalus.processing.fire.format.grid;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author thomas
 */
public class GridMapperTest {

    @Test
    public void testIsValidFirstHalfPixel() throws Exception {
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 1));
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 7));
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 10));
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 14));
        assertTrue(GridMapper.isValidFirstHalfPixel(1, 22, 15));
        assertFalse(GridMapper.isValidFirstHalfPixel(1, 22, 16));
        assertFalse(GridMapper.isValidFirstHalfPixel(1, 22, 22));
        assertFalse(GridMapper.isValidFirstHalfPixel(1, 22, 31));

        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 1));
        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 7));
        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 10));
        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 14));
        assertFalse(GridMapper.isValidSecondHalfPixel(31, 7, 15));
        assertTrue(GridMapper.isValidSecondHalfPixel(31, 7, 16));
        assertTrue(GridMapper.isValidSecondHalfPixel(31, 7, 22));
        assertTrue(GridMapper.isValidSecondHalfPixel(31, 7, 25));
        assertTrue(GridMapper.isValidSecondHalfPixel(31, 7, 31));

        assertTrue(GridMapper.isValidSecondHalfPixel(28, 7, 28));
    }
}