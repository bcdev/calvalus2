package com.bc.calvalus.portal.client;

import com.google.gwt.junit.client.GWTTestCase;

public class L3ProcessorPanelTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testParameters() {
        L3ProcessorPanel l3ProcessorPanel = new L3ProcessorPanel();
        String processorParameters = l3ProcessorPanel.getProcessorParameters();
        assertEquals("<parameters>" +
                             "<variables>chl, tsm, gelb</variables>" +
                             "<validMask>!l1_flags.INVALID && !l1p_flags.LAND && !l1p_flags.CLOUD</validMask>" +
                             "<aggregator>AVG</aggregator>" +
                             "<fromDate>2008-06-01</fromDate>" +
                             "<toDate>2008-06-07</toDate>" +
                             "<period>7</period>" +
                             "<fromLon>-180</fromLon>" +
                             "<toLon>+180</toLon>" +
                             "<fromLat>-90</fromLat>" +
                             "<toLat>+90</toLat>" +
                             "<resolution>0.0416667</resolution>" +
                             "</parameters>", processorParameters);
    }
}
