package com.bc.calvalus.portal.shared;

import junit.framework.TestCase;

public class DtoProductSetTest extends TestCase {

    public void testDefaultConstructorForGWTSerialisation() {
        DtoProductSet productSet = new DtoProductSet();
        assertEquals(null, productSet.getPath());
        assertEquals("", productSet.getName());
        assertEquals("", productSet.getType());
    }

}
