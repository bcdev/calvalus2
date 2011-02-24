package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalParameter;
import com.google.gwt.junit.client.GWTTestCase;

import java.util.List;

public class L3ProcessorPanelTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testParameterList() {
        L3ProcessorPanel l3ProcessorPanel = new L3ProcessorPanel();
        List<PortalParameter> parameterList = l3ProcessorPanel.getParameterList();
        assertNotNull(parameterList);
        assertEquals(14, parameterList.size());
    }

}
