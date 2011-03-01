package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProductionParameter;
import com.google.gwt.junit.client.GWTTestCase;

import java.util.List;

public class L3ProcessorPanelTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testParameterList() {
        L3ProcessorPanel l3ProcessorPanel = new L3ProcessorPanel();
        List<PortalProductionParameter> parameterList = l3ProcessorPanel.getParameterList();
        assertNotNull(parameterList);
        assertEquals(14, parameterList.size());
    }

}
