package com.bc.calvalus.wps2;

import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusProcessorExtractor;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wps2.jaxb.AcceptVersionsType;
import com.bc.calvalus.wps2.jaxb.AggregatorConfig;
import com.bc.calvalus.wps2.jaxb.Aggregators;
import com.bc.calvalus.wps2.jaxb.Capabilities;
import com.bc.calvalus.wps2.jaxb.CompositingType;
import com.bc.calvalus.wps2.jaxb.Execute;
import com.bc.calvalus.wps2.jaxb.ExecuteResponse;
import com.bc.calvalus.wps2.jaxb.GetCapabilities;
import com.bc.calvalus.wps2.jaxb.L3Parameters;
import com.bc.calvalus.wps2.jaxb.ProcessDescriptions;
import com.bc.calvalus.wps2.responses.DescribeProcessResponse;
import com.bc.calvalus.wps2.responses.ExecuteAcceptedResponse;
import com.bc.calvalus.wps2.responses.ExecuteFailedResponse;
import com.bc.calvalus.wps2.responses.ExecuteSuccessfulResponse;
import com.bc.calvalus.wps2.responses.GetCapabilitiesResponse;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hans on 12/08/2015.
 */
public class JaxbHelperTest {

    private JaxbHelper jaxbHelper;

    @Test
    public void testMarshal() throws Exception {
        jaxbHelper = new JaxbHelper();
        GetCapabilities getCapabilities = createGetCapabilitiesObejct();
        StringWriter writer = new StringWriter();
        jaxbHelper.marshal(getCapabilities, writer);

        System.out.println(writer.toString());
//        assertThat(writer.toString(), equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
//                                              "<wps:GetCapabilities service=\"TEST\" language=\"DE\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\">\n" +
//                                              "    <wps:AcceptVersions>\n" +
//                                              "        <ows:Version>version-1.0.0</ows:Version>\n" +
//                                              "        <ows:Version>version-1.0.1</ows:Version>\n" +
//                                              "    </wps:AcceptVersions>\n" +
//                                              "</wps:GetCapabilities>\n"));
    }

    @Ignore
    @Test
    public void testMarshalCapabilities() throws Exception {
        CalvalusProductionService calvalusProductionService = new CalvalusProductionService();
        CalvalusConfig calvalusConfig = new CalvalusConfig();
        ProductionService productionService = calvalusProductionService.createProductionService(calvalusConfig);
        CalvalusProcessorExtractor extractor = new CalvalusProcessorExtractor(productionService);
        List<Processor> processors = extractor.getProcessors();

        GetCapabilitiesResponse getCapabilitiesResponse = new GetCapabilitiesResponse();
        Capabilities capabilities = getCapabilitiesResponse.createGetCapabilitiesResponse(processors);

        StringWriter writer = new StringWriter();
        jaxbHelper = new JaxbHelper();
        jaxbHelper.marshal(capabilities, writer);

        System.out.println(writer.toString());

    }

    @Ignore
    @Test
    public void testMarshalProcessDescriptions() throws Exception {
        CalvalusProductionService calvalusProductionService = new CalvalusProductionService();
        CalvalusConfig calvalusConfig = new CalvalusConfig();
        ProductionService productionService = calvalusProductionService.createProductionService(calvalusConfig);
        CalvalusProcessorExtractor extractor = new CalvalusProcessorExtractor(productionService);
        Processor testProcessor = extractor.getProcessor(new ProcessorNameParser("case2-regional~1.5.3~Meris.Case2Regional"));

        DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
        ProcessDescriptions processDescriptions = describeProcessResponse.getDescribeProcessResponse(testProcessor, extractor);

        StringWriter writer = new StringWriter();
        jaxbHelper = new JaxbHelper();
        jaxbHelper.marshal(processDescriptions, writer);

        System.out.println(writer.toString());

    }

    @Ignore
    @Test
    public void testMarshalExecuteResponseSuccessful() throws Exception {
        ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
        List<String> productionResultUrls = new ArrayList<>();
        productionResultUrls.add("http://url1.nc");
        productionResultUrls.add("http://url2.nc");
        productionResultUrls.add("http://url3.nc");
        ExecuteResponse executeResponse = executeSuccessfulResponse.getExecuteResponse(productionResultUrls);

        StringWriter writer = new StringWriter();
        jaxbHelper = new JaxbHelper();
        jaxbHelper.marshal(executeResponse, writer);

        System.out.println(writer.toString());
    }

    @Ignore
    @Test
    public void testMarshalExecuteResponseAccepted() throws Exception {
        ExecuteAcceptedResponse acceptedResponse = new ExecuteAcceptedResponse();
        ExecuteResponse executeResponse = acceptedResponse.getExecuteResponse();

        StringWriter writer = new StringWriter();
        jaxbHelper = new JaxbHelper();
        jaxbHelper.marshal(executeResponse, writer);

        System.out.println(writer.toString());
    }

    @Ignore
    @Test
    public void testMarshalExecuteResponseFailed() throws Exception {
        ExecuteFailedResponse failedResponse = new ExecuteFailedResponse();
        ExecuteResponse executeResponse = failedResponse.getExecuteResponse();

        StringWriter writer = new StringWriter();
        jaxbHelper = new JaxbHelper();
        jaxbHelper.marshal(executeResponse, writer);

        System.out.println(writer.toString());
    }

    @Ignore
    @Test
    public void testUnmarshalExecuteRequest() throws Exception {
        String executeXmlString = getExecuteXmlString();
        InputStream requestInputStream = new ByteArrayInputStream(executeXmlString.getBytes());
        jaxbHelper = new JaxbHelper();
        Execute execute = (Execute) jaxbHelper.unmarshal(requestInputStream);

        System.out.println(execute.getIdentifier().getValue());
        System.out.println(execute.getResponseForm().getResponseDocument().getOutput().get(0).getIdentifier().getValue());
    }

    @Ignore
    @Test
    public void testUnmarshalProcessDescriptions() throws Exception {
        String l3ParametersXmlString = getProcessDescriptionsXmlString();
        InputStream processDescriptionsInputStream = new ByteArrayInputStream(l3ParametersXmlString.getBytes());
        jaxbHelper = new JaxbHelper();
        ProcessDescriptions processDescriptions = (ProcessDescriptions) jaxbHelper.unmarshal(processDescriptionsInputStream);

        System.out.println(processDescriptions.toString());
    }

    @Ignore
    @Test
    public void testUnmarshalL3Parameters() throws Exception {
        String l3ParametersXmlString = getL3ParametersXmlStringNotWorking();
        InputStream l3ParametersInputStream = new ByteArrayInputStream(l3ParametersXmlString.getBytes());
        jaxbHelper = new JaxbHelper();
        L3Parameters l3Parameters = (L3Parameters) jaxbHelper.unmarshal(l3ParametersInputStream);

        System.out.println(l3Parameters.getMaskExpr());
        System.out.println(l3Parameters.getPlanetaryGrid());
    }

    @Ignore
    @Test
    public void testMarshalL3Parameters() throws Exception {
        L3Parameters l3Parameters = new L3Parameters();
        l3Parameters.setCompositingType(CompositingType.BINNING);
        l3Parameters.setMaskExpr("maskExpr");
        l3Parameters.setNumRows(5);
        l3Parameters.setPlanetaryGrid("planetaryGrid");

        Aggregators aggregators = new Aggregators();
        AggregatorConfig aggregatorConfig1 = new AggregatorConfig();
        aggregatorConfig1.setType("AVG");
        aggregatorConfig1.setVarName("tsm");
        AggregatorConfig aggregatorConfig2 = new AggregatorConfig();
        aggregatorConfig2.setType("MIN_MAX");
        aggregatorConfig2.setVarName("chl_conc");
        aggregators.getAggregator().add(aggregatorConfig1);
        aggregators.getAggregator().add(aggregatorConfig2);
        l3Parameters.setAggregators(aggregators);

        StringWriter writer = new StringWriter();
        jaxbHelper = new JaxbHelper();
        jaxbHelper.marshal(l3Parameters, writer);

        System.out.println("writer.toString() = " + writer.toString());
    }

    private String getL3ParametersXmlStringV2() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<bc:parameters xmlns:bc=\"http://bc-schema.xsd\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "    <bc:planetaryGrid>planetaryGrid</bc:planetaryGrid>\n" +
               "    <bc:numRows>5</bc:numRows>\n" +
               "    <bc:compositingType>BINNING</bc:compositingType>\n" +
               "    <bc:maskExpr>maskExpr</bc:maskExpr>\n" +
               "    <bc:aggregators>\n" +
               "        <bc:type>AVG</bc:type>\n" +
               "        <bc:varName>tsm</bc:varName>\n" +
               "    </bc:aggregators>\n" +
               "    <bc:aggregators>\n" +
               "        <bc:type>MIN_MAX</bc:type>\n" +
               "        <bc:varName>chl_conc</bc:varName>\n" +
               "    </bc:aggregators>\n" +
               "</bc:parameters>";
    }


    private String getL3ParametersXmlStringNotWorking() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><cal:parameters xmlns:cal=\"http://bc-schema.xsd\" xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\t\t\t\t\t\t<cal:compositingType>MOSAICKING</cal:compositingType>\n" +
               "\t\t\t\t\t\t<cal:planetaryGrid>org.esa.beam.binning.support.PlateCarreeGrid</cal:planetaryGrid>\n" +
               "\t\t\t\t\t\t<cal:numRows>21600</cal:numRows>\n" +
               "\t\t\t\t\t\t<cal:superSampling>1</cal:superSampling>\n" +
               "\t\t\t\t\t\t<cal:maskExpr>!case2_flags.INVALID</cal:maskExpr>\n" +
               "\t\t\t\t\t\t<cal:aggregators><cal:aggregator><cal:type>AVG</cal:type><cal:varName>tsm</cal:varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator><cal:aggregator><cal:type>MIN_MAX</cal:type><cal:varName>chl_conc</cal:varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator><cal:aggregator><cal:type>AVG</cal:type><cal:varName>Z90_max</cal:varName>\n" +
               "\t\t\t\t\t\t\t</cal:aggregator>\n" +
               "\t\t\t\t\t\t</cal:aggregators>\n" +
               "\t\t\t\t\t</cal:parameters>";
    }

    private String getProcessDescriptionsXmlString() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<wps:ProcessDescriptions xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:wps=\"http://www.opengis.net/wps/1.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\t<ProcessDescription>\n" +
               "\t\t<ows:Identifier>case2-regional~1.5.3~Meris.Case2Regional</ows:Identifier>\n" +
               "\t\t<ows:Title>case2-regional~1.5.3~Meris.Case2Regional</ows:Title>\n" +
               "\t\t<ows:Abstract>\n" +
               "                &lt;p&gt;This is the case2-regional processor from BEAM 4.9.&lt;/p&gt;</ows:Abstract>\n" +
               "\t\t<DataInputs>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>productionName</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Production name</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>calvalus.calvalus.bundle</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Calvalus bundle version</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>calvalus-2.0b411</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>calvalus.beam.bundle</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Beam bundle version</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>beam-4.11.1-SNAPSHOT</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>doAtmosphericCorrection</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Whether or not to perform atmospheric correction.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>boolean</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>true</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>doSmileCorrection</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Whether to perform SMILE correction.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>boolean</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>true</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>outputTosa</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Toggles the output of TOSA reflectance.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>boolean</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>false</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>outputReflec</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Toggles the output of water leaving reflectance.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>boolean</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>true</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>outputReflecAs</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Select if reflectances shall be written as radiances or irradiances. The irradiances are compatible with standard MERIS product.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>RADIANCE_REFLECTANCES</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>outputPath</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Toggles the output of water leaving path reflectance.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>boolean</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>true</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>outputTransmittance</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Toggles the output of downwelling irradiance transmittance.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>boolean</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>false</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>outputNormReflec</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Toggles the output of normalised reflectances.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>boolean</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>false</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>landExpression</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>The arithmetic expression used for land detection.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>toa_reflec_10 &gt; toa_reflec_6 AND toa_reflec_13 &gt; 0.0475</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>cloudIceExpression</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>The arithmetic expression used for cloud/ice detection.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>toa_reflec_14 &gt; 0.2</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>algorithm</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>The algorithm used for IOP computation. Currently only 'REGIONAL' is valid</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>REGIONAL</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>tsmConversionExponent</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Exponent for conversion from TSM to B_TSM.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>1.0</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>tsmConversionFactor</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Factor for conversion from TSM to B_TSM.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>1.73</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>chlConversionExponent</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Exponent for conversion from A_PIG to CHL_CONC. </ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>1.04</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>chlConversionFactor</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Factor for conversion from A_PIG to CHL_CONC. </ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>21.0</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>spectrumOutOfScopeThreshold</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Threshold to indicate Spectrum is Out of Scope.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>4.0</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>invalidPixelExpression</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Expression defining pixels not considered for processing.</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<DefaultValue>agc_flags.INVALID</DefaultValue>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>inputPath</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Input path</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<ows:AllowedValues>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MER_RRG_1P/r03/${yyyy}/${MM}/${dd}/.*.N1</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MER_FSG_1P/v2013/${yyyy}/${MM}/${dd}/.*.N1</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MODISA_L1A/v1/${yyyy}/${MM}/${dd}/.*.L1A_LAC.bz2</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MODISA_L1A/baltic-sea/${yyyy}/${MM}/${dd}/A.*.L1A_LAC.x.hdf.bz2</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MODISA_L1A/north-sea/${yyyy}/${MM}/${dd}/A.*.L1A_LAC.x.hdf.bz2</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/Landsat8/v1/${yyyy}/${MM}/${dd}/.*.tar.gz</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/Landsat7-ETM/v1/${yyyy}/${MM}/${dd}/.*.tar.gz</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/Landsat45-TM/v1/${yyyy}/${MM}/${dd}/.*.tar.gz</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/ATS_TOA_1P/v1/${yyyy}/${MM}/${dd}/.*.N1</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/ATS_LST_2P/v1/${yyyy}/${MM}/${dd}/.*.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/AVHRR_L1B/noaa11/${yyyy}/${MM}/${dd}/.*.l1b,eodata/AVHRR_L1B/noaa14/${yyyy}/${MM}/${dd}/.*.l1b</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/SPOT_VGT_1P/v2/${yyyy}/${MM}/${dd}/.*.ZIP</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/OCM_L1B/hdf/${yyyy}/${MM}/${dd}/O.*hdf</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/projects/oc/Idepix4v1/${yyyy}/${MM}/L2_of_MER_..._1.*${yyyy}${MM}${dd}_.*seq$</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MODISA_L2_LAC/std/${yyyy}/${MM}/${dd}/.*.hdf</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MODIS_L1B/OBPG/${yyyy}/${MM}/${dd}/A.*L1B_LAC$</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/SEAWIFS_L1B/OBPG/${yyyy}/${MM}/${dd}/S.*L1B_LAC$</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/OC-CCI_chl/v1.0/${yyyy}/${MM}/${dd}/ESACCI-OC.*fv1.0.nc$</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/OC-CCI_chl/v2.0/${yyyy}/ESACCI-OC.*fv2.0.nc$</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/projects/waqss/MODISA_L2/.*/${yyyy}/${MM}/A${yyyy}${DDD}.*L2_TSM.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/projects/waqss/modis-l2/NSBS/${yyyy}/${MM}/${dd}/A${yyyy}${DDD}.*L2_sub</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/projects/waqss/viirs-l2.*/NSBS/${yyyy}/${MM}/${dd}/V${yyyy}${DDD}.*L2_sub</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h..v..-........-v2.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h..v..-........-v2.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h12v10-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v09-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h43v11-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h54v09-........-v2.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h12v10-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h36v09-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h43v11-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h54v09-........-v2.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h10v07-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h16v08-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h21v09-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h21v22-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h22v21-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h23v16-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h25v17-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h34v09-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h35v13-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h39v18-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h40v08-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h41v05-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h41v13-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h43v19-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h53v03-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h61v22-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h62v09-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h64v20-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h65v22-........-v2.0.nc,eodata/MERIS_SR_FR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h70v26-........-v2.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h10v07-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h16v08-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h21v09-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h21v22-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h22v21-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h23v16-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h25v17-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h34v09-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h35v13-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h39v18-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h40v08-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h41v05-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h41v13-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h43v19-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h53v03-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h61v22-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h62v09-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h64v20-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h65v22-........-v2.0.nc,eodata/MERIS_SR_RR/v2.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h70v26-........-v2.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h..v..-........-v1.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h..v..-........-v1.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h12v10-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v09-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h43v11-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h54v09-........-v1.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h12v10-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h36v09-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h43v11-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h54v09-........-v1.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h10v07-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h16v08-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h21v09-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h21v22-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h22v21-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h23v16-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h25v17-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h34v09-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h35v13-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h39v18-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h40v08-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h41v05-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h41v13-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h43v19-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h53v03-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h61v22-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h62v09-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h64v20-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h65v22-........-v1.0.nc,eodata/MERIS_SR_FR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-300m-P7D-h70v26-........-v1.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h10v07-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h16v08-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h21v09-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h21v22-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h22v21-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h23v16-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h25v17-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h34v09-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h35v13-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h39v18-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h40v08-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h41v05-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h41v13-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h43v19-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h53v03-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h61v22-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h62v09-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h64v20-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h65v22-........-v1.0.nc,eodata/MERIS_SR_RR/v1.0/${yyyy}/${yyyy}-${MM}-${dd}/ESACCI-LC-L3-SR-MERIS-1000m-P7D-h70v26-........-v1.0.nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/OSISAF-SEAICE/north/${yyyy}/${MM}/${dd}/ice.*nc</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">/calvalus/eodata/OSISAF-SEAICE/south/${yyyy}/${MM}/${dd}/ice.*nc</wps:Value>\n" +
               "\t\t\t\t\t</ows:AllowedValues>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>minDate</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Date from</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>maxDate</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Date to</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>periodLength</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Period length</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>regionWkt</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Region WKT</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>calvalus.l3.parameters</ows:Identifier>\n" +
               "\t\t\t\t<ComplexData>\n" +
               "\t\t\t\t\t<Default>\n" +
               "\t\t\t\t\t\t<Format>\n" +
               "\t\t\t\t\t\t\t<Schema>http://schema.xsd</Schema>\n" +
               "\t\t\t\t\t\t</Format>\n" +
               "\t\t\t\t\t</Default>\n" +
               "\t\t\t\t\t<Supported>\n" +
               "\t\t\t\t\t\t<Format>\n" +
               "\t\t\t\t\t\t\t<Schema>http://schema.xsd</Schema>\n" +
               "\t\t\t\t\t\t</Format>\n" +
               "\t\t\t\t\t</Supported>\n" +
               "\t\t\t\t</ComplexData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t\t<Input>\n" +
               "\t\t\t\t<ows:Identifier>calvalus.output.format</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>Calvalus output format</ows:Abstract>\n" +
               "\t\t\t\t<LiteralData>\n" +
               "\t\t\t\t\t<ows:DataType>string</ows:DataType>\n" +
               "\t\t\t\t\t<ows:AllowedValues>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">NetCDF</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">BEAM-DIMAP</wps:Value>\n" +
               "\t\t\t\t\t\t<wps:Value xsi:type=\"xs:string\">GeoTIFF</wps:Value>\n" +
               "\t\t\t\t\t</ows:AllowedValues>\n" +
               "\t\t\t\t</LiteralData>\n" +
               "\t\t\t</Input>\n" +
               "\t\t</DataInputs>\n" +
               "\t\t<ProcessOutputs>\n" +
               "\t\t\t<Output>\n" +
               "\t\t\t\t<ows:Identifier>productionResults</ows:Identifier>\n" +
               "\t\t\t\t<ows:Abstract>URL to the production result(s)</ows:Abstract>\n" +
               "\t\t\t\t<ComplexOutput>\n" +
               "\t\t\t\t\t<Default>\n" +
               "\t\t\t\t\t\t<Format>\n" +
               "\t\t\t\t\t\t\t<MimeType>binary</MimeType>\n" +
               "\t\t\t\t\t\t\t<Schema>http://schemaOutput.xsd</Schema>\n" +
               "\t\t\t\t\t\t</Format>\n" +
               "\t\t\t\t\t</Default>\n" +
               "\t\t\t\t</ComplexOutput>\n" +
               "\t\t\t</Output>\n" +
               "\t\t</ProcessOutputs>\n" +
               "\t</ProcessDescription>\n" +
               "</wps:ProcessDescriptions>";
    }

    private String getExecuteXmlString() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
               "\n" +
               "<wps:Execute service=\"WPS\"\n" +
               "             version=\"1.0.0\"\n" +
               "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\">\n" +
               "\n" +
               "\t<ows:Identifier>Calvalus</ows:Identifier>\n" +
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
               "\t\t\t<ows:Identifier>processorBundleName</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>case2-regional</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>processorBundleVersion</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>1.5.3</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>processorName</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:LiteralData>Meris.Case2Regional</wps:LiteralData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\t\t<wps:Input>\n" +
               "\t\t\t<ows:Identifier>processorParameters</ows:Identifier>\n" +
               "\t\t\t<wps:Data>\n" +
               "\t\t\t\t<wps:ComplexData>\n" +
               "\t\t\t\t\t<parameters>\n" +
               "\t\t\t\t\t\t<doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
               "\t\t\t\t\t\t<doSmileCorrection>true</doSmileCorrection>\n" +
               "\t\t\t\t\t\t<outputTosa>false</outputTosa>\n" +
               "\t\t\t\t\t\t<outputReflec>true</outputReflec>\n" +
               "\t\t\t\t\t\t<outputReflecAs>RADIANCE_REFLECTANCES</outputReflecAs>\n" +
               "\t\t\t\t\t\t<outputPath>true</outputPath>\n" +
               "\t\t\t\t\t\t<outputTransmittance>false</outputTransmittance>\n" +
               "\t\t\t\t\t\t<outputNormReflec>false</outputNormReflec>\n" +
               "\t\t\t\t\t\t<landExpression>toa_reflec_10 &gt; toa_reflec_6 AND toa_reflec_13 &gt; 0.0475</landExpression>\n" +
               "\t\t\t\t\t\t<cloudIceExpression>toa_reflec_14 &gt; 0.2</cloudIceExpression>\n" +
               "\t\t\t\t\t\t<algorithm>REGIONAL</algorithm>\n" +
               "\t\t\t\t\t\t<tsmConversionExponent>1.0</tsmConversionExponent>\n" +
               "\t\t\t\t\t\t<tsmConversionFactor>1.73</tsmConversionFactor>\n" +
               "\t\t\t\t\t\t<chlConversionExponent>1.04</chlConversionExponent>\n" +
               "\t\t\t\t\t\t<chlConversionFactor>21.0</chlConversionFactor>\n" +
               "\t\t\t\t\t\t<spectrumOutOfScopeThreshold>4.0</spectrumOutOfScopeThreshold>\n" +
               "\t\t\t\t\t\t<invalidPixelExpression>agc_flags.INVALID</invalidPixelExpression>\n" +
               "\t\t\t\t\t</parameters>\n" +
               "\t\t\t\t</wps:ComplexData>\n" +
               "\t\t\t</wps:Data>\n" +
               "\t\t</wps:Input>\n" +
               "\n" +
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
               "\t\t\t\t\t<parameters>\n" +
               "\t\t\t\t\t\t<compositingType>MOSAICKING</compositingType>\n" +
               "\t\t\t\t\t\t<planetaryGrid>org.esa.beam.binning.support.PlateCarreeGrid</planetaryGrid>\n" +
               "\t\t\t\t\t\t<numRows>21600</numRows>\n" +
               "\t\t\t\t\t\t<superSampling>1</superSampling>\n" +
               "\t\t\t\t\t\t<maskExpr>!case2_flags.INVALID</maskExpr>\n" +
               "\t\t\t\t\t\t<aggregators>\n" +
               "\t\t\t\t\t\t\t<aggregator>\n" +
               "\t\t\t\t\t\t\t\t<type>AVG</type>\n" +
               "\t\t\t\t\t\t\t\t<varName>tsm</varName>\n" +
               "\t\t\t\t\t\t\t</aggregator>\n" +
               "\t\t\t\t\t\t\t<aggregator>\n" +
               "\t\t\t\t\t\t\t\t<type>MIN_MAX</type>\n" +
               "\t\t\t\t\t\t\t\t<varName>chl_conc</varName>\n" +
               "\t\t\t\t\t\t\t</aggregator>\n" +
               "\t\t\t\t\t\t\t<aggregator>\n" +
               "\t\t\t\t\t\t\t\t<type>AVG</type>\n" +
               "\t\t\t\t\t\t\t\t<varName>Z90_max</varName>\n" +
               "\t\t\t\t\t\t\t</aggregator>\n" +
               "\t\t\t\t\t\t</aggregators>\n" +
               "\t\t\t\t\t</parameters>\n" +
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
               "\t\t<wps:Input>\n" +
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
               "</wps:Execute>";
    }

    private GetCapabilities createGetCapabilitiesObejct() {
        GetCapabilities getCapabilities = new GetCapabilities();
        getCapabilities.setLanguage("DE");
        getCapabilities.setService("TEST");
        AcceptVersionsType acceptVersionsType = new AcceptVersionsType();
        acceptVersionsType.getVersion().add(0, "version-1.0.0");
        acceptVersionsType.getVersion().add(1, "version-1.0.1");
        getCapabilities.setAcceptVersions(acceptVersionsType);
        return getCapabilities;
    }
}