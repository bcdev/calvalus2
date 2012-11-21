package com.bc.calvalus.processing.analysis;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Marco Peters
 */
public class QLConfigTest {

    @Test
    public void testFromXml() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("<QLConfig>\n");
        sb.append("<subSamplingX>3</subSamplingX>\n");
        sb.append("<subSamplingY>5</subSamplingY>\n");
        sb.append("<RGBAExpressions>var2 + log(var3), complicated expression, 1-3 + 5,constant</RGBAExpressions>\n");
        sb.append("<v1>0.45,1.34,5,77</v1>\n");
        sb.append("<v2>23,5.0007,.456</v2>\n");
        sb.append("<bandName>chl_conc</bandName>\n");
        sb.append("<cpdURL>http://www.allMycpds.com/chl.cpd</cpdURL>\n");
        sb.append("<imageType>SpecialType</imageType>\n");
        sb.append("<overlayURL>file://C:\\User\\home\\overlay.png</overlayURL>\n");
        sb.append("</QLConfig>\n");
        String xml = sb.toString();
        QLConfig qlConfig = QLConfig.fromXml(xml);
        assertEquals(3, qlConfig.getSubSamplingX());
        assertEquals(5, qlConfig.getSubSamplingY());
        String[] rgbaExpressions = qlConfig.getRGBAExpressions();
        assertEquals(4, rgbaExpressions.length);
        assertEquals("var2 + log(var3)", rgbaExpressions[0]);
        assertEquals(" complicated expression", rgbaExpressions[1]);
        assertEquals(" 1-3 + 5", rgbaExpressions[2]);
        assertEquals("constant", rgbaExpressions[3]);
        double[] v1Values = qlConfig.getV1();
        assertEquals(4, v1Values.length);
        assertEquals(0.45, v1Values[0], 1.0e-8);
        assertEquals(1.34, v1Values[1], 1.0e-8);
        assertEquals(5, v1Values[2], 1.0e-8);
        assertEquals(77, v1Values[3], 1.0e-8);
        double[] v2Values = qlConfig.getV2();
        assertEquals(3, v2Values.length);
        assertEquals(23, v2Values[0], 1.0e-8);
        assertEquals(5.0007, v2Values[1], 1.0e-8);
        assertEquals(0.456, v2Values[2], 1.0e-8);
        assertEquals("chl_conc", qlConfig.getBandName());
        assertEquals("http://www.allMycpds.com/chl.cpd", qlConfig.getCpdURL());
        assertEquals("SpecialType", qlConfig.getImageType());
        assertEquals("file://C:\\User\\home\\overlay.png", qlConfig.getOverlayURL());

    }
}
