package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.processing.beam.L3Config;
import com.bc.calvalus.processing.beam.WpsXmlGenerator;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Generates the WPS XML.
 */
public class WpsXmlGeneratorTest {

    @Test
    public void testL3WpsXml() throws WorkflowException, ProductionException {
        ProductionRequest productionRequest = L3ProductionTypeTest.createValidL3ProductionRequest();

        L3Config binningConfig = L3ProductionType.createBinningConfig(productionRequest);

        Map<String, Object> templateParameters = new HashMap<String, Object>(productionRequest.getProductionParameters());
        templateParameters.put("productionId", "ID_pi-pa-po");
        templateParameters.put("productionType", "A25F");
        templateParameters.put("productionName", "Wonderful L3");
        templateParameters.put("binningParameters", binningConfig);
        templateParameters.put("regionWkt", productionRequest.getRoiGeometry().toString());
        templateParameters.put("inputFiles", new String[]{
                "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F1.N1",
                "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F2.N1",
                "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F3.N1",
                "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F4.N1",
        });
        templateParameters.put("outputDir", "hdfs://cvmaster00:9000/calvalus/output/ewa/A25F_0");

        String xml = new WpsXmlGenerator().createL3WpsXml(templateParameters);
        assertNotNull(xml);

        // System.out.println(xml);

        assertTrue(xml.contains("<ows:Identifier>ID_pi-pa-po</ows:Identifier>"));
        assertTrue(xml.contains("<ows:Title>Wonderful L3</ows:Title>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.processor.package</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>beam</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.processor.version</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>4.9-SNAPSHOT</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.output.dir</ows:Identifier>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/output/ewa/A25F_0\"/>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.input</ows:Identifier>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F1.N1\"/>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F2.N1\"/>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F3.N1\"/>"));
        assertTrue(xml.contains("<wps:Reference xlink:href=\"hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2010/06/05/F4.N1\"/>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.geometry</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>POLYGON ((5 50, 25 50, 25 60, 5 60, 5 50))</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l2.operator</ows:Identifier>"));
        assertTrue(xml.contains("<wps:LiteralData>BandMaths</wps:LiteralData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l2.parameters</ows:Identifier>"));
        assertTrue(xml.contains("<wps:ComplexData><!-- no params --></wps:ComplexData>"));

        assertTrue(xml.contains("<ows:Identifier>calvalus.l3.parameters</ows:Identifier>"));
        assertTrue(xml.contains("<numRows>4320</numRows>"));
        assertTrue(xml.contains("<maskExpr>NOT INVALID</maskExpr>"));
        assertTrue(xml.contains("<numRows>4320</numRows>"));
        assertTrue(xml.contains("<superSampling>1</superSampling>"));
        assertTrue(xml.contains("<fillValue>-999.9</fillValue>"));

    }

}
