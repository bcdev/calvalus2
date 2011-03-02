package com.bc.calvalus.portal.client;

import com.google.gwt.junit.client.GWTTestCase;

import java.util.Map;

public class L3ProcessorPanelTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testParameterMap() {
        L3ProcessorPanel l3ProcessorPanel = new L3ProcessorPanel();
        Map<String, String> parameterMap = l3ProcessorPanel.getParameterMap();
        assertNotNull(parameterMap);
        assertEquals(14, parameterMap.size());
    }

}
