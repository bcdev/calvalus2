package com.bc.calvalus.processing.mosaic;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TileIndexWritableTest {

    @Test
    public void testCompareTo() throws Exception {
        final TileIndexWritable ti1 = new TileIndexWritable(28, 20, 144, 100);
        final TileIndexWritable ti1b = new TileIndexWritable(28, 20, 144, 100);
        final TileIndexWritable ti2 = new TileIndexWritable(28, 20, 144, 101);
        final TileIndexWritable ti3 = new TileIndexWritable(27, 22, 141, 102);
        final TileIndexWritable ti4 = new TileIndexWritable(28, 21, 141, 102);
        assertEquals(-1, ti1.compareTo(ti2));
        assertEquals(1, ti2.compareTo(ti1));
        assertEquals(0, ti1.compareTo(ti1b));
        assertEquals(0, ti1b.compareTo(ti1));
        assertEquals(1, ti3.compareTo(ti1));
        assertEquals(1, ti4.compareTo(ti2));
        assertEquals(-1, ti4.compareTo(ti3));
    }
}
