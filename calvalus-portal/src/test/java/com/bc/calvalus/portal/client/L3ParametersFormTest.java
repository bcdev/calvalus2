package com.bc.calvalus.portal.client;

import com.google.gwt.junit.client.GWTTestCase;

import java.util.Map;

public class L3ParametersFormTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testValueMap() {
        L3ParametersForm l3ParametersPanel = new L3ParametersForm();
        Map<String, String> valueMap = l3ParametersPanel.getValueMap();
        assertNotNull(valueMap);
        assertEquals(15, valueMap.size());

        assertEquals("chl_conc", valueMap.get("inputVariables"));
        assertEquals("!l1_flags.INVALID AND !l1_flags.LAND_OCEAN AND !l1p_flags.F_CLOUD", valueMap.get("maskExpr"));
        assertEquals("NaN", valueMap.get("fillValue"));
        assertEquals("AVG_ML", valueMap.get("aggregator"));
        assertEquals("0.5", valueMap.get("weightCoeff"));
        assertEquals("2008-06-01", valueMap.get("minDate"));
        assertEquals("2008-06-10", valueMap.get("maxDate"));
        assertEquals("10", valueMap.get("periodLength"));
        assertEquals("10", valueMap.get("compositingPeriodLength"));
        assertEquals("3", valueMap.get("minLon"));
        assertEquals("14.5", valueMap.get("maxLon"));
        assertEquals("52", valueMap.get("minLat"));
        assertEquals("56.5", valueMap.get("maxLat"));
        assertEquals("9.28", valueMap.get("resolution"));
        assertEquals("1", valueMap.get("superSampling"));
    }

}
