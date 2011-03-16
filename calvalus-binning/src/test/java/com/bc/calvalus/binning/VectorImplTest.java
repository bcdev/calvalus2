package com.bc.calvalus.binning;


import org.junit.Test;

import static org.junit.Assert.*;

public class VectorImplTest {

    @Test
    public void testToString() {
        VectorImpl v = new VectorImpl(new float[]{2.1F, 1.9F, 3.4F, 5.6F});
        assertEquals("[2.1, 1.9, 3.4, 5.6]", v.toString());
        v.setOffsetAndSize(0, 0);
        assertEquals("[]", v.toString());
        v.setOffsetAndSize(0, 3);
        assertEquals("[2.1, 1.9, 3.4]", v.toString());
        v.setOffsetAndSize(2, 2);
        assertEquals("[3.4, 5.6]", v.toString());
    }
}
