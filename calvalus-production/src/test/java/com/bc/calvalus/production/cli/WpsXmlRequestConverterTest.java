package com.bc.calvalus.production.cli;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.bc.calvalus.production.ProductionRequest;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author hans
 */
public class WpsXmlRequestConverterTest {

    @Test
    public void toXml() throws Exception {
        Map<String, String> productionParameters = new HashMap<>();
        productionParameters.put("calvalus.processor.package", "beam-meris-radiometry");
        productionParameters.put("calvalus.processor.version", "1.0-SNAPSHOT");
        productionParameters.put("calvalus.input.format", "BEAM-DIMAP");
        productionParameters.put("calvalus.output.dir", "hdfs://master00:9000/calvalus/outputs/meris-l2beam-99");
        productionParameters.put("calvalus.input", "hdfs://master00:9000/calvalus/outputs/meris-l2beam-99");
        productionParameters.put("calvalus.l2.operator", "Meris.CorrectRadiometry");
        productionParameters.put("calvalus.l2.parameters", "<parameters>\n" +
                                                           "   <doSmile>true</doSmile>\n" +
                                                           "    <reproVersion>AUTO_DETECT</reproVersion>\n" +
                                                           "</parameters>");
        productionParameters.put("calvalus.plainText.parameter", "<parameters>\n" +
                                                                 "This is a multiline\n" +
                                                                 "Textfield\n" +
                                                                 "</parameters>");
        ProductionRequest productionRequest = new ProductionRequest("L2Plus", "userName", productionParameters);

        String requestXml = new WpsXmlRequestConverter(productionRequest).toXml();

        assertThat(requestXml, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
                                       "\n" +
                                       "<wps:Execute service=\"WPS\"\n" +
                                       "             version=\"1.0.0\"\n" +
                                       "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
                                       "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
                                       "             xmlns:xlink=\"http://www.w3.org/1999/xlink\"\n" +
                                       "             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                                       "             xsi:schemaLocation=\"http://www.opengis.net/wps/1.0.0 ogc/wps/1.0.0/wpsExecute_request.xsd\">\n  <ows:Identifier>L2Plus</ows:Identifier>\n" +
                                       "  <wps:DataInputs>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.processor.package</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>beam-meris-radiometry</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.output.dir</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>hdfs://master00:9000/calvalus/outputs/meris-l2beam-99</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.processor.version</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>1.0-SNAPSHOT</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.input</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>hdfs://master00:9000/calvalus/outputs/meris-l2beam-99</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.input.format</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>BEAM-DIMAP</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.l2.operator</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>Meris.CorrectRadiometry</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.l2.parameters</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData><parameters>\n" +
                                       "   <doSmile>true</doSmile>\n" +
                                       "    <reproVersion>AUTO_DETECT</reproVersion>\n</parameters></wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.plainText.parameter</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData><parameters>\n" +
                                       "This is a multiline\n" +
                                       "Textfield\n" +
                                       "</parameters></wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "  </wps:DataInputs>\n" +
                                       "</wps:Execute>"));
    }

    @Test
    public void toXmlMultipleInputPath() throws Exception {
        Map<String, String> productionParameters = new HashMap<>();
        productionParameters.put("calvalus.processor.package", "beam-meris-radiometry");
        productionParameters.put("calvalus.processor.version", "1.0-SNAPSHOT");
        productionParameters.put("calvalus.input.format", "BEAM-DIMAP");
        productionParameters.put("calvalus.output.dir", "hdfs://master00:9000/calvalus/outputs/meris-l2beam-99");
        productionParameters.put("calvalus.input", "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_011806_000026382028_00332_12410_0000.N1," +
                                                   "hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_021806_000026382028_00332_12410_0000.N1");
        productionParameters.put("calvalus.l2.operator", "Meris.CorrectRadiometry");
        productionParameters.put("calvalus.l2.parameters", "<parameters>\n" +
                                                           "   <doSmile>true</doSmile>\n" +
                                                           "    <reproVersion>AUTO_DETECT</reproVersion>\n" +
                                                           "</parameters>");
        productionParameters.put("calvalus.plainText.parameter", "<parameters>\n" +
                                                                 "This is a multiline\n" +
                                                                 "Textfield\n" +
                                                                 "</parameters>");
        ProductionRequest productionRequest = new ProductionRequest("L2Plus", "userName", productionParameters);

        String requestXml = new WpsXmlRequestConverter(productionRequest).toXml();

        assertThat(requestXml, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
                                       "\n" +
                                       "<wps:Execute service=\"WPS\"\n" +
                                       "             version=\"1.0.0\"\n" +
                                       "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
                                       "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
                                       "             xmlns:xlink=\"http://www.w3.org/1999/xlink\"\n" +
                                       "             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                                       "             xsi:schemaLocation=\"http://www.opengis.net/wps/1.0.0 ogc/wps/1.0.0/wpsExecute_request.xsd\">\n  <ows:Identifier>L2Plus</ows:Identifier>\n" +
                                       "  <wps:DataInputs>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.processor.package</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>beam-meris-radiometry</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.output.dir</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>hdfs://master00:9000/calvalus/outputs/meris-l2beam-99</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.processor.version</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>1.0-SNAPSHOT</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.input</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_011806_000026382028_00332_12410_0000.N1,hdfs://master00:9000/calvalus/eodata/MER_RR__1P/r03/2004/07/15/MER_RR__1PRACR20040715_021806_000026382028_00332_12410_0000.N1</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.input.format</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>BEAM-DIMAP</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.l2.operator</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData>Meris.CorrectRadiometry</wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.l2.parameters</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData><parameters>\n" +
                                       "   <doSmile>true</doSmile>\n" +
                                       "    <reproVersion>AUTO_DETECT</reproVersion>\n</parameters></wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "    <wps:Input>\n" +
                                       "      <ows:Identifier>calvalus.plainText.parameter</ows:Identifier>\n" +
                                       "      <ows:Title/>\n" +
                                       "      <wps:Data>\n" +
                                       "        <wps:LiteralData><parameters>\n" +
                                       "This is a multiline\n" +
                                       "Textfield\n" +
                                       "</parameters></wps:LiteralData>\n" +
                                       "      </wps:Data>\n" +
                                       "    </wps:Input>\n" +
                                       "  </wps:DataInputs>\n" +
                                       "</wps:Execute>"));
    }

    @Test
    public void toXmlWhenNoDataInputs() throws Exception {
        Map<String, String> productionParameters = new HashMap<>();
        ProductionRequest productionRequest = new ProductionRequest("L2Plus", "userName", productionParameters);

        String requestXml = new WpsXmlRequestConverter(productionRequest).toXml();

        assertThat(requestXml, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
                                       "\n" +
                                       "<wps:Execute service=\"WPS\"\n" +
                                       "             version=\"1.0.0\"\n" +
                                       "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
                                       "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
                                       "             xmlns:xlink=\"http://www.w3.org/1999/xlink\"\n" +
                                       "             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
                                       "             xsi:schemaLocation=\"http://www.opengis.net/wps/1.0.0 ogc/wps/1.0.0/wpsExecute_request.xsd\">\n  <ows:Identifier>L2Plus</ows:Identifier>\n" +
                                       "  <wps:DataInputs/>\n" +
                                       "</wps:Execute>"));
    }
}