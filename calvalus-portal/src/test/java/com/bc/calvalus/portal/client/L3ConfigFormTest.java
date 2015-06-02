package com.bc.calvalus.portal.client;

import com.google.gwt.junit.client.GWTTestCase;

import java.util.Map;

public class L3ConfigFormTest extends GWTTestCase {

    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    @Override
    protected void gwtSetUp() throws Exception {
        TestHelper.assertMapsApiLoaded();
    }

    public void testValueMap() {
        L3ConfigForm l3ConfigForm = new L3ConfigForm(null);
        Map<String, String> valueMap = l3ConfigForm.getValueMap();
        assertNotNull(valueMap);
        assertEquals(6, valueMap.size());

        assertEquals("0", valueMap.get("variables.count"));
        assertEquals("", valueMap.get("maskExpr"));
        assertEquals("10", valueMap.get("periodLength"));
        assertEquals("10", valueMap.get("compositingPeriodLength"));
        assertEquals("9.28", valueMap.get("resolution"));
        assertEquals("1", valueMap.get("superSampling"));
    }

}
