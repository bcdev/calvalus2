package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Generates the WPS XML.
 */
public class WpsXmlGeneratorTest {

    @Test
    public void testL3WpsXml() throws ProductionException {
        ProductionRequest productionRequest = L3ProcessingRequestTest.createValidL3ProductionRequest();
        L3ProcessingRequest processingRequest = new L3ProcessingRequest(productionRequest) {
            @Override
            public String[] getInputFiles() throws ProductionException {
                return new String[]{"fileA", "fileB", "fileC"};
            }
        };
        String xml = new WpsXmlGenerator().createL3WpsXml(processingRequest);
        assertNotNull(xml);

        System.out.println(xml);

        assertTrue(xml.contains("<ows:Identifier>1234</ows:Identifier>"));
        assertTrue(xml.contains("<ows:Title>Wonderful L3</ows:Title>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.processor.package</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>beam</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.processor.version</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>4.9-SNAPSHOT</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.output.dir</ows:Identifier>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/outputs/calvalus-level3-output\"/>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.input</ows:Identifier>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"fileA\"/>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"fileB\"/>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"fileC\"/>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l2.operator</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>BandMaths</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l2.parameters</ows:Identifier>"));
        assertTrue(xml.contains("<wps:ComplexData><!-- no params --></wps:ComplexData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l3.parameters</ows:Identifier>"));
        assertTrue(xml.contains("<numRows>4320</numRows>"));
        assertTrue(xml.contains("<maskExpr>NOT INVALID</maskExpr>"));
        assertTrue(xml.contains("<bbox>5,50,25,60</bbox>"));
        assertTrue(xml.contains("<numRows>4320</numRows>"));
        assertTrue(xml.contains("<superSampling>1</superSampling>"));

    }
}
