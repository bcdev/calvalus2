package com.bc.calvalus.processing.fire.format.grid;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author thomas
 */
public class AbstractGridMapperTest {

    @Test
    public void testIsValidFirstHalfPixel() throws Exception {
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 1));
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 7));
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 10));
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 14));
        assertTrue(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 15));
        assertFalse(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 16));
        assertFalse(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 22));
        assertFalse(AbstractGridMapper.isValidFirstHalfPixel(1, 22, 31));

        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 1));
        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 7));
        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 10));
        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 14));
        assertFalse(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 15));
        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 16));
        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 22));
        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 25));
        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(31, 7, 31));

        assertTrue(AbstractGridMapper.isValidSecondHalfPixel(28, 7, 28));
    }
}