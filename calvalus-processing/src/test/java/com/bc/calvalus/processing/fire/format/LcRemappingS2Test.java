package com.bc.calvalus.processing.fire.format;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LcRemappingS2Test {

    @Test
    public void isInBurnableLcClass() throws Exception {
        assertFalse(LcRemappingS2.isInBurnableLcClass(0));
        assertTrue(LcRemappingS2.isInBurnableLcClass(1));
        assertTrue(LcRemappingS2.isInBurnableLcClass(2));
        assertTrue(LcRemappingS2.isInBurnableLcClass(3));
        assertTrue(LcRemappingS2.isInBurnableLcClass(4));
        assertTrue(LcRemappingS2.isInBurnableLcClass(5));
        assertTrue(LcRemappingS2.isInBurnableLcClass(6));
        assertFalse(LcRemappingS2.isInBurnableLcClass(7));
        assertFalse(LcRemappingS2.isInBurnableLcClass(8));
        assertFalse(LcRemappingS2.isInBurnableLcClass(9));
        assertFalse(LcRemappingS2.isInBurnableLcClass(10));
        assertFalse(LcRemapping.isInBurnableLcClass(LcRemapping.remap(210)));


    }

}