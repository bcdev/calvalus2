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

package com.bc.calvalus.processing;

import com.bc.calvalus.processing.beam.BeamOperatorAdapter;
import com.bc.ceres.binding.dom.DomElement;
import org.esa.snap.framework.gpf.GPF;
import org.esa.snap.framework.gpf.annotations.ParameterBlockConverter;
import org.esa.snap.gpf.operators.standard.BandMathsOp;
import org.esa.snap.util.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test for {@link JobUtils}.
 */
public class JobUtilsTest {

    @Before
    public void before() {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }


    private String readFromResource(String name) throws IOException, SAXException, ParserConfigurationException {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(name));
        return FileUtils.readText(inputStreamReader).trim();
    }

    @Test
    public void convertComplexRequestToParametersMap() throws Exception {
        String level2OperatorName = "BandMaths";
        String level2Parameters = readFromResource("bandmaths.xml");

        DomElement element = new ParameterBlockConverter().convertXmlToDomElement(level2Parameters);
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

        Map<String, Object> operatorParameters = BeamOperatorAdapter.getOperatorParameterMap(level2OperatorName, level2Parameters);
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
