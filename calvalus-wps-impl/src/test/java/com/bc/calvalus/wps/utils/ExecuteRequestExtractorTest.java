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
               "<wps:Execute service=\"WPS\"\n" +
               "             version=\"1.0.0\"\n" +
               "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "\t\t\t xmlns:cal=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\"\n" +
               "             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\n" +
               "\t<ows:Identifier>case2-regional~1.5.3~Meris.Case2Regional</ows:Identifier>\n" +
               "\n" +
               "\t<DataInputs>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>productionType</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>L3</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.calvalus.bundle</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>calvalus-2.0b411</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.beam.bundle</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>beam-4.11.1-SNAPSHOT</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>productionName</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>Ocean Colour test</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>doAtmosphericCorrection</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>true</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>doSmileCorrection</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>true</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>outputTosa</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>false</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>outputReflec</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>true</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>outputReflecAs</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>RADIANCE_REFLECTANCES</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>outputPath</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>true</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>outputTransmittance</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>false</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>outputNormReflec</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>false</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>landExpression</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>toa_reflec_10 &gt; toa_reflec_6 AND toa_reflec_13 &gt; 0.0475</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>cloudIceExpression</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>toa_reflec_14 &gt; 0.2</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>algorithm</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>REGIONAL</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>tsmConversionExponent</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>1.0</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>tsmConversionFactor</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>1.73</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>chlConversionExponent</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>1.04</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>chlConversionFactor</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>21.0</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>spectrumOutOfScopeThreshold</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>4.0</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>invalidPixelExpression</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>agc_flags.INVALID</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>inputPath</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>/calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/MER_RR__1P....${yyyy}${MM}${dd}.*N1$</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>minDate</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>2009-06-01</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>maxDate</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>2009-06-30</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>periodLength</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>30</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>regionWKT</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>polygon((10.00 54.00,  14.27 53.47,  20.00 54.00, 21.68 54.77, 22.00 56.70, 24.84 56.70, 30.86 60.01, 26.00 62.00, 26.00 66.00, 22.00 66.00, 10.00 60.00, 10.00 54.00))</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.l3.parameters</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<ComplexData>\n" +
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
               "\t\t\t\t</ComplexData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.output.dir</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>/calvalus/home/hans/ocean-colour-test</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.output.format</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>NetCDF4</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.system.beam.pixelGeoCoding.useTiling</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>true</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.hadoop.mapreduce.job.queuename</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>test</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>     \n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.hadoop.mapreduce.map.maxattempts</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>1</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "      <Input>\n" +
               "\t\t\t<ows:Identifier>autoStaging</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData>true</LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t</DataInputs>\n" +
               "\t<ResponseForm>\n" +
               "\t\t<ResponseDocument storeExecuteResponse=\"true\" status=\"true\">\n" +
               "\t\t\t<Output>\n" +
               "\t\t\t\t<ows:Identifier>productionResults</ows:Identifier>\n" +
               "\t\t\t</Output>\n" +
               "\t\t</ResponseDocument>\n" +
               "\t</ResponseForm>\n" +
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
               "\t<DataInputs>\n" +
               "\t\t<Input>\n" +
               "\t\t\t<ows:Identifier>productionType</ows:Identifier>\n" +
               "\t\t\t<Data>\n" +
               "\t\t\t\t<LiteralData></LiteralData>\n" +
               "\t\t\t</Data>\n" +
               "\t\t</Input>\n" +
               "\t</DataInputs>\n" +
               "\t<ResponseForm>\n" +
               "\t\t<ResponseDocument storeExecuteResponse=\"true\" status=\"true\">\n" +
               "\t\t\t<Output>\n" +
               "\t\t\t\t<ows:Identifier>productionResults</ows:Identifier>\n" +
               "\t\t\t</Output>\n" +
               "\t\t</ResponseDocument>\n" +
               "\t</ResponseForm>\n" +
               "\n" +
               "</wps:Execute>\n";
    }

}