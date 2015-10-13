package com.bc.calvalus.wpsrest;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.wpsrest.jaxb.Execute;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by hans on 15/09/2015.
 */
public class ExecuteRequestExtractorTest {

    private ExecuteRequestExtractor requestExtractor;

    @Test
    public void canGetParameterValues() throws Exception {
        String executeRequestString = getExecuteRequestString();
        InputStream requestInputStream = new ByteArrayInputStream(executeRequestString.getBytes());
        JaxbHelper jaxbHelper = new JaxbHelper();
        Execute execute = (Execute) jaxbHelper.unmarshal(requestInputStream);

        requestExtractor = new ExecuteRequestExtractor(execute);

        assertThat(requestExtractor.getValue("productionType"), equalTo("L3"));
        assertThat(requestExtractor.getValue("calvalus.beam.bundle"), equalTo("beam-4.11.1-SNAPSHOT"));
        assertThat(requestExtractor.getValue("landExpression"), equalTo("toa_reflec_10 > toa_reflec_6 AND toa_reflec_13 > 0.0475"));
    }

    @Test
    public void canGetParameterMap() throws Exception {
        String executeRequestString = getExecuteRequestString();
        InputStream requestInputStream = new ByteArrayInputStream(executeRequestString.getBytes());
        JaxbHelper jaxbHelper = new JaxbHelper();
        Execute execute = (Execute) jaxbHelper.unmarshal(requestInputStream);

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

    private String getExecuteRequestString() {
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
               "\t\t\t\t<wps:LiteralData>L3</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.calvalus.bundle</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>calvalus-2.0b411</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.beam.bundle</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>beam-4.11.1-SNAPSHOT</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>productionName</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>Ocean Colour test</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>doAtmosphericCorrection</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>true</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>doSmileCorrection</ows:Identifier>\n" +
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
               "\t\t\t<ows:Identifier>outputReflecAs</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>RADIANCE_REFLECTANCES</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>outputPath</ows:Identifier>\n" +
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
               "\t\t\t<ows:Identifier>outputNormReflec</ows:Identifier>\n" +
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
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>cloudIceExpression</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>toa_reflec_14 &gt; 0.2</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>algorithm</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>REGIONAL</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>tsmConversionExponent</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>1.0</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>tsmConversionFactor</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>1.73</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>chlConversionExponent</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>1.04</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>chlConversionFactor</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>21.0</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>spectrumOutOfScopeThreshold</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>4.0</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>invalidPixelExpression</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>agc_flags.INVALID</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>inputPath</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>/calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/MER_RR__1P....${yyyy}${MM}${dd}.*N1$</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>minDate</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>2009-06-01</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>maxDate</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>2009-06-30</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>periodLength</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>30</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>regionWKT</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>polygon((10.00 54.00,  14.27 53.47,  20.00 54.00, 21.68 54.77, 22.00 56.70, 24.84 56.70, 30.86 60.01, 26.00 62.00, 26.00 66.00, 22.00 66.00, 10.00 60.00, 10.00 54.00))</wps:LiteralData>\n" +
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
               "\t\t\t\t\t\t\t\t<cal:type>AVG</cal:type>\n" +
               "\t\t\t\t\t\t\t\t<cal:varName>tsm</cal:varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator>\n" +
               "\t\t\t\t\t\t\t<cal:aggregator>\n" +
               "\t\t\t\t\t\t\t\t<cal:type>MIN_MAX</cal:type>\n" +
               "\t\t\t\t\t\t\t\t<cal:varName>chl_conc</cal:varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator>\n" +
               "\t\t\t\t\t\t\t<cal:aggregator>\n" +
               "\t\t\t\t\t\t\t\t<cal:type>AVG</cal:type>\n" +
               "\t\t\t\t\t\t\t\t<cal:varName>Z90_max</cal:varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator>\n" +
               "\t\t\t\t\t\t</cal:aggregators>\n" +
               "\t\t\t\t\t</cal:parameters>\n" +
               "\t\t\t\t</wps:ComplexData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.output.dir</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>/calvalus/home/hans/ocean-colour-test</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.output.format</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>NetCDF4</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.system.beam.pixelGeoCoding.useTiling</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>true</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.hadoop.mapreduce.job.queuename</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>test</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>     \n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>calvalus.hadoop.mapreduce.map.maxattempts</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>1</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "      <wps:Input>\n" +
               "\t\t\t<ows:Identifier>autoStaging</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>true</wps:LiteralData>\n" +
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
}