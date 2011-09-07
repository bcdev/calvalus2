package com.bc.calvalus.portal.shared;

import junit.framework.TestCase;

public class DtoProductSetTest extends TestCase {

    public void testDefaultConstructorForGWTSerialisation() {
        DtoProductSet productSet = new DtoProductSet();
        assertEquals(null, productSet.getName());
        assertEquals(null, productSet.getPath());
        assertEquals(null, productSet.getMinDate());
        assertEquals(null, productSet.getMaxDate());
    }

}
