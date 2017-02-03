/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import com.bc.ceres.binding.dom.XppDomElement;
import com.thoughtworks.xstream.io.xml.xppdom.XppDom;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;
import org.junit.Test;

import static org.junit.Assert.*;

public class SnapGraphAdapterTest {

    @Test
    public void testAppDataConversion() throws Exception {
        String xml = "" +
                "      <executeParameters>\n" +
                "        <outputFiles>\n" +
                "          <outputFile>20090601_MER_094859_c2rcc-0.15_wqua_Bodensee_myusertagexample.tif</outputFile>\n" +
                "          <outputFile>20090601_MER_094859_c2rcc-0.15_rtoa_Bodensee_myusertagexample.tif</outputFile>\n" +
                "          <outputFile>20090601_MER_094859_c2rcc-0.15_iops_Bodensee_myusertagexample.tif</outputFile>\n" +
                "          <outputFile>20090601_MER_094859_c2rcc-0.15_unce_Bodensee_myusertagexample.tif</outputFile>\n" +
                "        </outputFiles>\n" +
                "        <outputNodes>\n" +
                "          <outputNode id=\"wqwrite\" parameter=\"file\"/>\n" +
                "        </outputNodes>\n" +
                "        <archiveFile>20090601_MER_094859_c2rcc-0.15______Bodensee_myusertagexample.zip</archiveFile>\n" +
                "      </executeParameters>\n";

        SnapGraphAdapter.CalvalusGraphApplicationData appData = SnapGraphAdapter.CalvalusGraphApplicationData.fromXml(xml);
        assertNotNull(appData);
        assertEquals("20090601_MER_094859_c2rcc-0.15______Bodensee_myusertagexample.zip", appData.archiveFile);
        assertEquals(4, appData.outputFiles.length);
        assertEquals("20090601_MER_094859_c2rcc-0.15_wqua_Bodensee_myusertagexample.tif", appData.outputFiles[0]);
        assertEquals(1, appData.outputNodes.length);
        SnapGraphAdapter.OutputNodeRef outputNodeZero = appData.outputNodes[0];
        assertEquals("wqwrite", outputNodeZero.nodeId);
        assertEquals("file", outputNodeZero.parameter);

        DomElement dom = new ParameterBlockConverter().convertXmlToDomElement(xml);
        assertSame(XppDomElement.class, dom.getClass());
        XppDomElement xppDomElement = (XppDomElement) dom;
        XppDom xppDom = xppDomElement.getXppDom();
        SnapGraphAdapter.CalvalusGraphApplicationData appData2 = SnapGraphAdapter.CalvalusGraphApplicationData.fromDom(xppDom);
        assertNotNull(appData2);
        assertEquals("20090601_MER_094859_c2rcc-0.15______Bodensee_myusertagexample.zip", appData2.archiveFile);

    }
}