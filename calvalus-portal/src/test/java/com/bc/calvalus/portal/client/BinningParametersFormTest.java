package com.bc.calvalus.portal.client;

import com.google.gwt.junit.client.GWTTestCase;

import java.util.Map;

public class BinningParametersFormTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testValueMap() {
        BinningParametersForm binningParametersForm = new BinningParametersForm();
        Map<String, String> valueMap = binningParametersForm.getValueMap();
        assertNotNull(valueMap);
        assertEquals(10, valueMap.size());

        // todo - make this work again! (nf)
        /*
        assertEquals("1", valueMap.get("variables.count"));
        assertEquals("chl_conc", valueMap.get("variables.0.name"));
        assertEquals("AVG_ML", valueMap.get("variables.0.aggregator"));
        assertEquals("0.5", valueMap.get("variables.0.weightCoeff"));
        assertEquals("NaN", valueMap.get("variables.0.fillValue"));
        */
        assertEquals("!l1_flags.INVALID AND !l1_flags.LAND_OCEAN", valueMap.get("variables.0.maskExpr"));
        assertEquals("10", valueMap.get("periodLength"));
        assertEquals("10", valueMap.get("compositingPeriodLength"));
        assertEquals("9.28", valueMap.get("resolution"));
        assertEquals("1", valueMap.get("superSampling"));
    }

}
