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
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.gpf.operators.standard.BandMathsOp;
import org.esa.beam.util.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStreamReader;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test for {@link BeamOperatorMapper}.
 */
public class BeamOperatorMapperTest {

    @Before
    public void before() {
         GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }

    @Test
    public void convertSimpleRequestToParametersMap() throws Exception {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream("radiometry-request.xml"));
        String wpsXML = FileUtils.readText(inputStreamReader).trim();
        XmlDoc request = new XmlDoc(wpsXML);

        Map<String,Object> operatorParameters = BeamOperatorMapper.getOperatorParameters(request);
        assertNotNull(operatorParameters);
        assertEquals(2, operatorParameters.size());

        Object doSmileObj = operatorParameters.get("doSmile");
        assertNotNull(doSmileObj);
        assertSame(String.class, doSmileObj.getClass());
        assertEquals(true, Boolean.parseBoolean((String) doSmileObj));

        Object reproVersionObj = operatorParameters.get("reproVersion");
        assertNotNull(reproVersionObj);
        assertSame(String.class, reproVersionObj.getClass());
        assertEquals("AUTO_DETECT", reproVersionObj);

    }

    @Test
    public void convertComplexRequestToParametersMap() throws Exception {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream("bandmaths-request.xml"));
        String wpsXML = FileUtils.readText(inputStreamReader).trim();
        XmlDoc request = new XmlDoc(wpsXML);

        Map<String,Object> operatorParameters = BeamOperatorMapper.getOperatorParameters(request);
        assertNotNull(operatorParameters);
        assertEquals(2, operatorParameters.size());

        Object variablesObj = operatorParameters.get("variables");
        assertNotNull(variablesObj);
        assertSame(BandMathsOp.Variable[].class, variablesObj.getClass());

    }
}
