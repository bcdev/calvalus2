package com.bc.calvalus.portal.client;

import com.google.gwt.junit.client.GWTTestCase;

import java.util.Map;

public class BinningParametersFormTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testValueMap() {
        BinningParametersForm l3ParametersPanel = new BinningParametersForm();
        Map<String, String> valueMap = l3ParametersPanel.getValueMap();
        assertNotNull(valueMap);
        assertEquals(9, valueMap.size());

        assertEquals("chl_conc", valueMap.get("inputVariables"));
        assertEquals("!l1_flags.INVALID AND !l1_flags.LAND_OCEAN", valueMap.get("maskExpr"));
        assertEquals("NaN", valueMap.get("fillValue"));
        assertEquals("AVG_ML", valueMap.get("aggregator"));
        assertEquals("0.5", valueMap.get("weightCoeff"));
        assertEquals("10", valueMap.get("periodLength"));
        assertEquals("10", valueMap.get("compositingPeriodLength"));
        assertEquals("9.28", valueMap.get("resolution"));
        assertEquals("1", valueMap.get("superSampling"));
    }

}
