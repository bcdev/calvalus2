package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.TestProcessingService;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Generates the WPS XML.
 */
public class WpsXmlGeneratorTest {

    @Test
    public void testL3WpsXml() throws ProductionException {
        L3ProcessingRequestFactory l3ProcessingRequestFactory = new L3ProcessingRequestFactory(new TestProcessingService(),
                                                                                               "/opt/tomcat/webapps/calvalus/staging");
        ProductionRequest productionRequest = L3ProcessingRequestTest.createValidL3ProductionRequest();
        L3ProcessingRequest[] processingRequests = l3ProcessingRequestFactory.createProcessingRequests("A25F", "ewa", productionRequest);

        String xml = new WpsXmlGenerator().createL3WpsXml("ID_pi-pa-po", "Wonderful L3", processingRequests[0]);
        assertNotNull(xml);

        // System.out.println(xml);

        assertTrue(xml.contains("<ows:Identifier>ID_pi-pa-po</ows:Identifier>"));
        assertTrue(xml.contains("<ows:Title>Wonderful L3</ows:Title>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.processor.package</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>beam</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.processor.version</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>4.9-SNAPSHOT</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.output.dir</ows:Identifier>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/output/ewa-A25F/out\"/>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.input</ows:Identifier>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F1.N1\"/>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F2.N1\"/>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F3.N1\"/>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F4.N1\"/>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l2.operator</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>BandMaths</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l2.parameters</ows:Identifier>"));
        assertTrue(xml.contains("<wps:ComplexData><!-- no params --></wps:ComplexData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l3.parameters</ows:Identifier>"));
        assertTrue(xml.contains("<numRows>4320</numRows>"));
        assertTrue(xml.contains("<maskExpr>NOT INVALID</maskExpr>"));
        assertTrue(xml.contains("<fillValue>NaN</fillValue>"));
        assertTrue(xml.contains("<bbox>5,50,25,60</bbox>"));
        assertTrue(xml.contains("<numRows>4320</numRows>"));
        assertTrue(xml.contains("<superSampling>1</superSampling>"));

    }

}
