package com.bc.calvalus.processing.fire;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author thomas
 * @author marcop
 */
public class FireGridInputFormatTest {

    @Test
    public void testGetTileIndices() throws Exception {

        assertArrayEquals(new int[]{7, 11}, FireGridInputFormat.getTileIndices("BA_PIX_MER_v07h11_200806_v4.0.tif"));
        assertArrayEquals(new int[]{12, 5}, FireGridInputFormat.getTileIndices("BA_PIX_MER_v12h05_200806_v4.0.tif"));
        assertArrayEquals(new int[]{5, 5}, FireGridInputFormat.getTileIndices("BA_PIX_MER_v05h05_200806_v4.0.tif"));
        assertArrayEquals(new int[]{11, 10}, FireGridInputFormat.getTileIndices("BA_PIX_MER_v11h10_200806_v4.0.tif"));

    }
}