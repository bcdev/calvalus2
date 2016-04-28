package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.PixelPos;
import org.junit.Test;

import java.awt.Rectangle;

import static org.junit.Assert.*;

/**
 * @author thomas
 */
public class FireGridMapperTest {

    @Test
    public void testIsValidFirstHalfPixel() throws Exception {
        assertTrue(FireGridMapper.isValidFirstHalfPixel(1, 22, 1));
        assertTrue(FireGridMapper.isValidFirstHalfPixel(1, 22, 7));
        assertTrue(FireGridMapper.isValidFirstHalfPixel(1, 22, 10));
        assertTrue(FireGridMapper.isValidFirstHalfPixel(1, 22, 14));
        assertTrue(FireGridMapper.isValidFirstHalfPixel(1, 22, 15));
        assertFalse(FireGridMapper.isValidFirstHalfPixel(1, 22, 16));
        assertFalse(FireGridMapper.isValidFirstHalfPixel(1, 22, 22));
        assertFalse(FireGridMapper.isValidFirstHalfPixel(1, 22, 31));

        assertFalse(FireGridMapper.isValidSecondHalfPixel(31, 7, 1));
        assertFalse(FireGridMapper.isValidSecondHalfPixel(31, 7, 7));
        assertFalse(FireGridMapper.isValidSecondHalfPixel(31, 7, 10));
        assertFalse(FireGridMapper.isValidSecondHalfPixel(31, 7, 14));
        assertFalse(FireGridMapper.isValidSecondHalfPixel(31, 7, 15));
        assertTrue(FireGridMapper.isValidSecondHalfPixel(31, 7, 16));
        assertTrue(FireGridMapper.isValidSecondHalfPixel(31, 7, 22));
        assertTrue(FireGridMapper.isValidSecondHalfPixel(31, 7, 25));
        assertTrue(FireGridMapper.isValidSecondHalfPixel(31, 7, 31));

        assertTrue(FireGridMapper.isValidSecondHalfPixel(28, 7, 28));
    }

    @Test
    public void testGetSourceRect() throws Exception {
        assertEquals(new Rectangle(5, 5, 90, 90), FireGridMapper.getSourceRect(new PixelPos(50, 50)));

        assertEquals(new Rectangle(-5, -5, 90, 90), FireGridMapper.getSourceRect(new PixelPos(40, 40)));
        assertEquals(new Rectangle(15, 15, 90, 90), FireGridMapper.getSourceRect(new PixelPos(60, 60)));

        assertEquals(new Rectangle(5, -35, 90, 90), FireGridMapper.getSourceRect(new PixelPos(50, 10)));
    }

}