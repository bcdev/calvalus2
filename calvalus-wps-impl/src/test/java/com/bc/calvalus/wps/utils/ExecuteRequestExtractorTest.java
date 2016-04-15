package com.bc.calvalus.wps.utils;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.wps.exceptions.WpsMissingParameterValueException;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ObjectFactory;
import com.bc.wps.utilities.JaxbHelper;
import org.junit.*;
import org.junit.rules.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * @author hans
 */
public class ExecuteRequestExtractorTest {

    private ExecuteRequestExtractor requestExtractor;

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Test
    public void canGetParameterValues() throws Exception {
        String executeRequestString = getExecuteRequestString();
        InputStream requestInputStream = new ByteArrayInputStream(executeRequestString.getBytes());
        Execute execute = (Execute) JaxbHelper.unmarshal(requestInputStream, new ObjectFactory());

        requestExtractor = new ExecuteRequestExtractor(execute);

        assertThat(requestExtractor.getValue("productionType"), equalTo("L3"));
        assertThat(requestExtractor.getValue("calvalus.beam.bundle"), equalTo("beam-4.11.1-SNAPSHOT"));
        assertThat(requestExtractor.getValue("landExpression"), equalTo("toa_reflec_10 > toa_reflec_6 AND toa_reflec_13 > 0.0475"));
    }

    @Test
    public void canGetBoundingBoxValuesAsPolygon() throws Exception {
        String executeRequestString = getExecuteRequestStringWithBoundingBoxValue();
        InputStream requestInputStream = new ByteArrayInputStream(executeRequestString.getBytes());
        Execute execute = (Execute) JaxbHelper.unmarshal(requestInputStream, new ObjectFactory());

        requestExtractor = new ExecuteRequestExtractor(execute);

        assertThat(requestExtractor.getValue("regionWKT"), equalTo("POLYGON((100.00000 -10.00000,100.00000 0.00000,110.00000 0.00000,110.00000 -10.00000,100.00000 -10.00000))"));
    }

    @Test
    public void canGetParameterMap() throws Exception {
        String executeRequestString = getExecuteRequestString();
        InputStream requestInputStream = new ByteArrayInputStream(executeRequestString.getBytes());
        Execute execute = (Execute) JaxbHelper.unmarshal(requestInputStream, new ObjectFactory());

        requestExtractor = new ExecuteRequestExtractor(execute);

        Map<String, String> parameterMap = requestExtractor.getInputParametersMapRaw();

        assertThat(parameterMap.get("doAtmosphericCorrection"), equalTo("true"));
        assertThat(parameterMap.get("outputTosa"), equalTo("false"));
        assertThat(parameterMap.get("outputTransmittance"), equalTo("false"));
        assertThat(parameterMap.get("calvalus.l3.parameters"), equalTo("<parameters>\n" +
                                                                       "<planetaryGrid>org.esa.beam.binning.support.PlateCarreeGrid</planetaryGrid>\n" +
                                                                       "<numRows>21600</numRows>\n" +
                                                                       "<compositingType>MOSAICKING</compositingType>\n" +
                                                                       "<superSampling>1</superSampling>\n" +
                                                                       "<maskExpr>!case2_flags.INVALID</maskExpr>\n" +
                                                                       "<aggregators>\n" +
                                                                       "<aggregator>\n" +
                                                                       "<type>AVG</type>\n" +
                                                                       "<varName>tsm</varName>\n" +
                                                                       "</aggregator>\n" +
                                                                       "<aggregator>\n" +
                                                                       "<type>MIN_MAX</type>\n" +
                                                                       "<varName>chl_conc</varName>\n" +
                                                                       "</aggregator>\n" +
                                                                       "<aggregator>\n" +
                                                                       "<type>AVG</type>\n" +
                                                                       "<varName>Z90_max</varName>\n" +
                                                                       "</aggregator>\n" +
                                                                       "</aggregators>\n" +
                                                                       "</parameters>"));

    }

    @Test
    public void canThrowExceptionWhenParameterValueIsMissing() throws Exception {
        String executeRequestString = getExecuteRequestStringWithEmptyInputValue();
        InputStream requestInputStream = new ByteArrayInputStream(executeRequestString.getBytes());
        Execute execute = (Execute) JaxbHelper.unmarshal(requestInputStream, new ObjectFactory());

        thrownException.expect(WpsMissingParameterValueException.class);
        thrownException.expectMessage("Missing value from parameter : productionType");

        requestExtractor = new ExecuteRequestExtractor(execute);
    }

    private String getExecuteRequestString() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
               "\n" +
               "<wps:Execute xsi:schemaLocation=\"http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsExecute_request.xsd\"" +
               "             service=\"WPS\"\n" +
               "             version=\"1.0.0\"\n" +
               "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "\t\t\t xmlns:cal=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\"\n" +
               "             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" +
               "\n" +
               "\t<ows:Identifier>case2-regional~1.5.3~Meris.Case2Regional</ows:Identifier>\n" +
               "\n" +
               "\t<wps:DataInputs>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>productionType</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>L3</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.beam.bundle</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>beam-4.11.1-SNAPSHOT</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>doAtmosphericCorrection</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>true</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>outputTosa</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>false</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>outputReflec</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>true</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>outputTransmittance</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>false</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>landExpression</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>toa_reflec_10 &gt; toa_reflec_6 AND toa_reflec_13 &gt; 0.0475</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.l3.parameters</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:ComplexData>\n" +
               "\t\t\t\t\t<cal:parameters>\n" +
               "\t\t\t\t\t\t<cal:compositingType>MOSAICKING</cal:compositingType>\n" +
               "\t\t\t\t\t\t<cal:planetaryGrid>org.esa.beam.binning.support.PlateCarreeGrid</cal:planetaryGrid>\n" +
               "\t\t\t\t\t\t<cal:numRows>21600</cal:numRows>\n" +
               "\t\t\t\t\t\t<cal:superSampling>1</cal:superSampling>\n" +
               "\t\t\t\t\t\t<cal:maskExpr>!case2_flags.INVALID</cal:maskExpr>\n" +
               "\t\t\t\t\t\t<cal:aggregators>\n" +
               "\t\t\t\t\t\t\t<cal:aggregator>\n" +
               "\t\t\t\t\t\t\t\t<type xsi:type=\"xsi:string\">AVG</type>\n" +
               "\t\t\t\t\t\t\t\t<varName xsi:type=\"xsi:string\">tsm</varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator>\n" +
               "\t\t\t\t\t\t\t<cal:aggregator>\n" +
               "\t\t\t\t\t\t\t\t<type xsi:type=\"xsi:string\">MIN_MAX</type>\n" +
               "\t\t\t\t\t\t\t\t<varName xsi:type=\"xsi:string\">chl_conc</varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator>\n" +
               "\t\t\t\t\t\t\t<cal:aggregator>\n" +
               "\t\t\t\t\t\t\t\t<type xsi:type=\"xsi:string\">AVG</type>\n" +
               "\t\t\t\t\t\t\t\t<varName xsi:type=\"xsi:string\">Z90_max</varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator>\n" +
               "\t\t\t\t\t\t</cal:aggregators>\n" +
               "\t\t\t\t\t</cal:parameters>\n" +
               "\t\t\t\t</wps:ComplexData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\n" +
               "\t</wps:DataInputs>\n" +
               "\t<wps:ResponseForm>\n" +
               "\t\t<wps:ResponseDocument storeExecuteResponse=\"true\" status=\"true\">\n" +
               "\t\t\t<wps:Output>\n" +
               "\t\t\t\t<ows:Identifier>productionResults</ows:Identifier>\n" +
               "\t\t\t</wps:Output>\n" +
               "\t\t</wps:ResponseDocument>\n" +
               "\t</wps:ResponseForm>\n" +
               "\n" +
               "</wps:Execute>\n";
    }

    private String getExecuteRequestStringWithEmptyInputValue() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
               "\n" +
               "<wps:Execute service=\"WPS\"\n" +
               "             version=\"1.0.0\"\n" +
               "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "\t\t\t xmlns:cal=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\n" +
               "\t<ows:Identifier>case2-regional~1.5.3~Meris.Case2Regional</ows:Identifier>\n" +
               "\n" +
               "\t<wps:DataInputs>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>productionType</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData></wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t</wps:DataInputs>\n" +
               "\t<wps:ResponseForm>\n" +
               "\t\t<wps:ResponseDocument storeExecuteResponse=\"true\" status=\"true\">\n" +
               "\t\t\t<wps:Output>\n" +
               "\t\t\t\t<ows:Identifier>productionResults</ows:Identifier>\n" +
               "\t\t\t</wps:Output>\n" +
               "\t\t</wps:ResponseDocument>\n" +
               "\t</wps:ResponseForm>\n" +
               "\n" +
               "</wps:Execute>\n";
    }

    private String getExecuteRequestStringWithBoundingBoxValue() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
               "\n" +
               "<wps:Execute service=\"WPS\"\n" +
               "             version=\"1.0.0\"\n" +
               "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "             xmlns:cal=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\n" +
               "\t<ows:Identifier>urbantep-subsetting~1.0~Subset</ows:Identifier>\n" +
               "\n" +
               "\t<wps:DataInputs>\n" +
               "      \t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>regionWKT</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<ows:BoundingBoxData>\n" +
               "                  <ows:LowerCorner>100 -10</ows:LowerCorner>\n" +
               "                  <ows:UpperCorner>110 0</ows:UpperCorner>\n" +
               "                  <ows:crs>urn:ogc:def:crs:EPSG:6:6:4326</ows:crs>\n" +
               "                </ows:BoundingBoxData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t\n" +
               "\t</wps:DataInputs>\n" +
               "\t<wps:ResponseForm>\n" +
               "\t\t<wps:ResponseDocument storeExecuteResponse=\"true\" status=\"true\">\n" +
               "\t\t\t<wps:Output>\n" +
               "\t\t\t\t<ows:Identifier>productionResults</ows:Identifier>\n" +
               "\t\t\t</wps:Output>\n" +
               "\t\t</wps:ResponseDocument>\n" +
               "\t</wps:ResponseForm>\n" +
               "</wps:Execute>";
    }

}