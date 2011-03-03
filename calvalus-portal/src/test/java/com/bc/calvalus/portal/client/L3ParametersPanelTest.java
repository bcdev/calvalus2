package com.bc.calvalus.portal.client;

import com.google.gwt.junit.client.GWTTestCase;

import java.util.Map;

public class L3ParametersPanelTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testValueMap() {
        L3ParametersPanel l3ParametersPanel = new L3ParametersPanel();
        Map<String, String> valueMap = l3ParametersPanel.getValueMap();
        assertNotNull(valueMap);
        assertEquals(15, valueMap.size());

        assertEquals("NaN", valueMap.get("fillValue"));
    }

}
