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

import com.bc.calvalus.processing.WpsConfig;
import org.esa.beam.util.io.FileUtils;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.*;

/**
 * Test for {@link com.bc.calvalus.processing.WpsConfig}.
 */
public class WpsConfigTest {

    @Test
    public void singleInputProduct() throws Exception {
        WpsConfig wpsConfig = createFromResource("radiometry-request.xml");
        String[] requestInputPaths = wpsConfig.getRequestInputPaths();
        assertNotNull(requestInputPaths);
        assertEquals(1, requestInputPaths.length);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_011806_000026382028_00332_12410_0000.N1", requestInputPaths[0]);
    }

    @Test
    public void multipleInputProducts() throws Exception {
        WpsConfig wpsConfig = createFromResource("bandmaths-request.xml");
        String[] requestInputPaths = wpsConfig.getRequestInputPaths();
        assertNotNull(requestInputPaths);
        assertEquals(3, requestInputPaths.length);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_011806_000026382028_00332_12410_0000.N1", requestInputPaths[0]);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2004/08/15/MER_RR__1PRACR20040715_011806_000026382028_00332_12410_0000.N1", requestInputPaths[1]);
        assertEquals("hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2004/09/15/MER_RR__1PRACR20040715_011806_000026382028_00332_12410_0000.N1", requestInputPaths[2]);
    }

    @Test
    public void outputPath() throws Exception {
        WpsConfig wpsConfig = createFromResource("radiometry-request.xml");
        String requestOutputDir = wpsConfig.getRequestOutputDir();
        assertNotNull(requestOutputDir);
        assertEquals("hdfs://cvmaster00:9000/calvalus/outputs/meris-l2beam-99", requestOutputDir);
    }

    @Test
    public void getProcessorPackage() throws Exception {
        WpsConfig wpsConfig = createFromResource("radiometry-request.xml");
        String processorPackage = wpsConfig.getProcessorPackage();
        assertNotNull(processorPackage);
        assertEquals("beam-meris-radiometry-1.0-SNAPSHOT", processorPackage);
    }

    @Test
    public void getIdentifier() throws Exception {
        WpsConfig wpsConfig = createFromResource("radiometry-request.xml");
        String identifier = wpsConfig.getIdentifier();
        assertNotNull(identifier);
        assertEquals("Meris.CorrectRadiometry", identifier);
    }

    @Test
    public void getRoiWkt() throws Exception {
        WpsConfig wpsConfig = createFromResource("radiometry-request.xml");
        String roiWkt = wpsConfig.getGeometry();
        assertEquals("POLYGON((23.0 42.0, 11.0 42.0, 11.0 22.0, 23.0 22.0, 23.0 42.0)", roiWkt);
    }

    private WpsConfig createFromResource(String name) throws IOException, SAXException, ParserConfigurationException {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(name));
        String wpsXML = FileUtils.readText(inputStreamReader).trim();
        return new WpsConfig(wpsXML);
    }
}
