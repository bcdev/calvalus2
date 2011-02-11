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

import com.bc.calvalus.processing.shellexec.XmlDoc;
import com.bc.ceres.binding.dom.DomElement;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.gpf.operators.standard.BandMathsOp;
import org.esa.beam.meris.radiometry.equalization.ReprocessingVersion;
import org.esa.beam.util.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStreamReader;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test for {@link BeamOperatorConfiguration}.
 */
public class BeamOperatorConfigurationTest {

    @Before
    public void before() {
         GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }

    @Test
    public void convertSimpleRequestToParametersMap() throws Exception {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream("radiometry-request.xml"));
        String wpsXML = FileUtils.readText(inputStreamReader).trim();

        BeamOperatorConfiguration opConfig = new BeamOperatorConfiguration(wpsXML);
        DomElement element = opConfig.getOperatorParametersDomElement();
        assertNotNull(element);
        assertEquals("parameters", element.getName());
        assertEquals(2, element.getChildCount());
        DomElement child1 = element.getChild(0);
        assertEquals("doSmile", child1.getName());
        assertEquals("true", child1.getValue());
        DomElement child2 = element.getChild(1);
        assertEquals("reproVersion", child2.getName());
        assertEquals("AUTO_DETECT", child2.getValue());

        Map<String,Object> operatorParameters = opConfig.getOperatorParameters();
        assertNotNull(operatorParameters);
        assertEquals(2, operatorParameters.size());

        Object doSmileObj = operatorParameters.get("doSmile");
        assertNotNull(doSmileObj);
        assertSame(Boolean.class, doSmileObj.getClass());
        assertEquals(true, doSmileObj);

        Object reproVersionObj = operatorParameters.get("reproVersion");
        assertNotNull(reproVersionObj);
        assertSame(ReprocessingVersion.class, reproVersionObj.getClass());
        assertEquals(ReprocessingVersion.AUTO_DETECT, reproVersionObj);

    }

    @Test
    public void convertComplexRequestToParametersMap() throws Exception {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream("bandmaths-request.xml"));
        String wpsXML = FileUtils.readText(inputStreamReader).trim();
        XmlDoc request = new XmlDoc(wpsXML);

        // Check if we can extract parameters element
        BeamOperatorConfiguration opConfig = new BeamOperatorConfiguration(wpsXML);
        DomElement element = opConfig.getOperatorParametersDomElement();
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

        Map<String,Object> operatorParameters = opConfig.getOperatorParameters();
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
}
