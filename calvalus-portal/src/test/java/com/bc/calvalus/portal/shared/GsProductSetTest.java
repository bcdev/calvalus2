package com.bc.calvalus.portal.shared;

import junit.framework.TestCase;

public class GsProductSetTest extends TestCase {

    public void testDefaultConstructorForGWTSerialisation() {
        GsProductSet productSet = new GsProductSet();
        assertEquals(null, productSet.getId());
        assertEquals("", productSet.getName());
        assertEquals("", productSet.getType());
    }

}
