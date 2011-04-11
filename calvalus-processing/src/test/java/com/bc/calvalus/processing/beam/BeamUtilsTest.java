/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.beam;

import com.bc.ceres.binding.dom.DomElement;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.gpf.operators.standard.BandMathsOp;
import org.esa.beam.meris.radiometry.equalization.ReprocessingVersion;
import org.esa.beam.util.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Test for {@link BeamUtils}.
 */
public class BeamUtilsTest {

    @Before
    public void before() {
         GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }


    private String readFromResource(String name) throws IOException, SAXException, ParserConfigurationException {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(name));
        return FileUtils.readText(inputStreamReader).trim();
    }

    @Test
    public void convertSimpleRequestToParametersMap() throws Exception {
        String level2OperatorName = "Meris.CorrectRadiometry";
        String level2Parameters = readFromResource("radiometry.xml");

        DomElement element = BeamUtils.createDomElement(level2Parameters);
        assertNotNull(element);
        assertEquals("parameters", element.getName());
        assertEquals(4, element.getChildCount());

        DomElement child1 = element.getChild(0);
        assertEquals("doCalibration", child1.getName());
        assertEquals("false", child1.getValue());

        DomElement child2 = element.getChild(1);
        assertEquals("doSmile", child2.getName());
        assertEquals("true", child2.getValue());

        DomElement child3 = element.getChild(2);
        assertEquals("doEqualization", child3.getName());
        assertEquals("false", child3.getValue());

        DomElement child4 = element.getChild(3);
        assertEquals("reproVersion", child4.getName());
        assertEquals("REPROCESSING_2", child4.getValue());

        Map<String,Object> operatorParameters = BeamUtils.getLevel2ParameterMap(level2OperatorName, level2Parameters);
        assertNotNull(operatorParameters);
        assertEquals(4, operatorParameters.size());

        Object obj = operatorParameters.get("doCalibration");
        assertNotNull(obj);
        assertSame(Boolean.class, obj.getClass());
        assertEquals(false, obj);

        obj = operatorParameters.get("doCalibration");
        assertNotNull(obj);
        assertSame(Boolean.class, obj.getClass());
        assertEquals(false, obj);

        obj = operatorParameters.get("doCalibration");
        assertNotNull(obj);
        assertSame(Boolean.class, obj.getClass());
        assertEquals(false, obj);

        obj = operatorParameters.get("reproVersion");
        assertNotNull(obj);
        assertSame(ReprocessingVersion.class, obj.getClass());
        assertEquals(ReprocessingVersion.REPROCESSING_2, obj);

    }

    @Test
    public void convertComplexRequestToParametersMap() throws Exception {
        String level2OperatorName = "BandMaths";
        String level2Parameters = readFromResource("bandmaths.xml");

        DomElement element = BeamUtils.createDomElement(level2Parameters);
        assertNotNull(element);
        assertEquals("parameters", element.getName());
        assertEquals(2, element.getChildCount());

        DomElement variablesChild = element.getChild(0);
        assertEquals("variables", variablesChild.getName());
        assertEquals(3, variablesChild.getChildCount());
        DomElement child1 = variablesChild.getChild(0);
        assertEquals("variable", child1.getName());
        assertEquals(3, child1.getChildCount());
        assertEquals("name", child1.getChild(0).getName());
        assertEquals("SOLAR_FLUX_13", child1.getChild(0).getValue());
        assertEquals("type", child1.getChild(1).getName());
        assertEquals("value", child1.getChild(2).getName());
        DomElement child2 = variablesChild.getChild(1);
        assertEquals("variable", child2.getName());
        assertEquals(3, child2.getChildCount());
        assertEquals("name", child2.getChild(0).getName());
        assertEquals("SOLAR_FLUX_14", child2.getChild(0).getValue());
        assertEquals("type", child2.getChild(1).getName());
        assertEquals("value", child2.getChild(2).getName());
        DomElement child3 = variablesChild.getChild(2);
        assertEquals("variable", child3.getName());
        assertEquals(3, child3.getChildCount());
        assertEquals("name", child3.getChild(0).getName());
        assertEquals("PI", child3.getChild(0).getValue());
        assertEquals("type", child3.getChild(1).getName());
        assertEquals("value", child3.getChild(2).getName());

        assertEquals("targetBands", element.getChild(1).getName());
        assertEquals(2, element.getChild(1).getChildCount());

        // Now check if the full value conversion is ok

        Map<String,Object> operatorParameters = BeamUtils.getLevel2ParameterMap(level2OperatorName, level2Parameters);
        assertNotNull(operatorParameters);
        assertEquals(2, operatorParameters.size());

        Object variablesObj = operatorParameters.get("variables");
        assertNotNull(variablesObj);
        assertSame(BandMathsOp.Variable[].class, variablesObj.getClass());
        BandMathsOp.Variable[] variables = (BandMathsOp.Variable[]) variablesObj;
        assertEquals(3, variables.length);
        assertEquals("SOLAR_FLUX_13", variables[0].name);
        assertEquals("SOLAR_FLUX_14", variables[1].name);
        assertEquals("PI", variables[2].name);

        // Object targetBandsObj = operatorParameters.get("targetBands");
        Object targetBandsObj = operatorParameters.get("targetBandDescriptors");
        assertNotNull(targetBandsObj);
        assertSame(BandMathsOp.BandDescriptor[].class, targetBandsObj.getClass());
        BandMathsOp.BandDescriptor[] targetBands = (BandMathsOp.BandDescriptor[]) targetBandsObj;
        assertEquals(2, targetBands.length);
        assertEquals("reflec_13", targetBands[0].name);
        assertEquals("reflec_14", targetBands[1].name);
    }

    @Test
    public void testConvertProperties() throws Exception {
        Properties p = new Properties();
        assertEquals("", BeamUtils.convertProperties(p));

        p.setProperty("name1", "value1");
        p.setProperty("name2", "value2");
        assertEquals("name1=value1,name2=value2", BeamUtils.convertProperties(p));

        Map<String, String> map = BeamUtils.convertProperties("");
        assertEquals(0, map.size());

        map = BeamUtils.convertProperties((String)null);
        assertEquals(0, map.size());

        map = BeamUtils.convertProperties("name1=value1,name2=value2");
        assertEquals(2, map.size());
        assertTrue(map.containsKey("name1"));
        assertTrue(map.containsKey("name2"));
        assertEquals("value1", map.get("name1"));
        assertEquals("value2", map.get("name2"));
    }
}
