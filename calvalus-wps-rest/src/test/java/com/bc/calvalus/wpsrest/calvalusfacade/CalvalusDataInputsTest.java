package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.exception.WpsInvalidParameterValueException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by hans on 15/09/2015.
 */
public class CalvalusDataInputsTest {

    private ExecuteRequestExtractor mockExecuteRequestExtractor;
    private Processor mockProcessor;
    private ProductSet[] productSets;

    /**
     * Class under test.
     */
    private CalvalusDataInputs calvalusDataInputs;

    @Before
    public void setUp() throws Exception {
        mockExecuteRequestExtractor = mock(ExecuteRequestExtractor.class);
        mockProcessor = mock(Processor.class);
        when(mockProcessor.getInputProductTypes()).thenReturn(new String[]{"MERIS RR  r03 L1b 2002-2012"});
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getName()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        when(mockProductSet.getPath()).thenReturn("eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1");
        when(mockProductSet.getProductType()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        productSets = new ProductSet[]{mockProductSet};
    }

    @Test
    public void canGetInputMapFormatted() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockProcessor.getDefaultSnapBundle()).thenReturn("snap-2.0.0");
        when(mockProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.snap.bundle"), equalTo("snap-2.0.0"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionName"), equalTo("dummyProductionName"));
    }

    @Test
    public void canGetProductionParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockProcessor.getDefaultSnapBundle()).thenReturn("snap-2.0.0");
        when(mockProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getValue("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getValue("calvalus.snap.bundle"), equalTo("snap-2.0.0"));
        assertThat(calvalusDataInputs.getValue("productionName"), equalTo("dummyProductionName"));
    }

    @Test
    public void canGetProcessorInfoParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        when(mockProcessor.getBundleName()).thenReturn("beam-idepix");
        when(mockProcessor.getBundleVersion()).thenReturn("2.0.9");
        when(mockProcessor.getName()).thenReturn("Idepix.Water");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo("beam-idepix"));
        assertThat(calvalusDataInputs.getValue("processorBundleVersion"), equalTo("2.0.9"));
        assertThat(calvalusDataInputs.getValue("processorName"), equalTo("Idepix.Water"));
    }

    @Test
    public void canHandleNoProcessorInfoParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo(null));
        assertThat(calvalusDataInputs.getValue("processorBundleVersion"), equalTo(null));
        assertThat(calvalusDataInputs.getValue("processorName"), equalTo(null));
    }

    @Test
    public void canGetProductSetParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductSetParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("inputPath"), equalTo("/calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1"));
        assertThat(calvalusDataInputs.getValue("minDate"), equalTo("2009-06-01"));
        assertThat(calvalusDataInputs.getValue("maxDate"), equalTo("2009-06-30"));
        assertThat(calvalusDataInputs.getValue("periodLength"), equalTo("30"));
        assertThat(calvalusDataInputs.getValue("regionWKT"), equalTo("polygon((10.00 54.00,  14.27 53.47))"));
        assertThat(calvalusDataInputs.getValue("calvalus.output.format"), equalTo("NetCDF4"));
    }

    @Test
    public void canTransformProcessorParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProcessorParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        ParameterDescriptor[] mockParameterDescriptors = getMockParameterDescriptorArray();
        when(mockProcessor.getParameterDescriptors()).thenReturn(mockParameterDescriptors);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo("<parameters>\n" +
                                                                               "<doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
                                                                               "<doSmileCorrection>false</doSmileCorrection>\n" +
                                                                               "<outputNormReflec>true</outputNormReflec>\n" +
                                                                               "</parameters>"));
    }

    @Test
    public void canIgnoreProcessorParametersThatAreNotProvided() throws Exception {
        Map<String, String> mockInputMapRaw = getLessProcessorParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        ParameterDescriptor[] mockParameterDescriptors = getMockParameterDescriptorArray();
        when(mockProcessor.getParameterDescriptors()).thenReturn(mockParameterDescriptors);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo("<parameters>\n" +
                                                                               "<doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
                                                                               "</parameters>"));
    }

    @Test
    public void canGetDefaultProcessorParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockProcessor.getDefaultParameters()).thenReturn(getMockDefaultParameters());

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo("########################\n" +
                                                                               "# POLYMER COMMAND FILE #\n" +
                                                                               "########################\n" +
                                                                               "# NB: paths can be given as relative or absolute\n" +
                                                                               "# lines starting with character '#' are ignored\n" +
                                                                               "# INPUT/OUTPUT ######\n" +
                                                                               "L1_FORMAT MERIS\n" +
                                                                               "# output format: HDF, NETCDF, NETCDF_CLASSIC\n" +
                                                                               "OUTPUT_FORMAT NETCDF\n" +
                                                                               "# uncomments this to process only a subset of the rows in the L1 file\n" +
                                                                               "# PROCESS_ROWS 7700 9000\n" +
                                                                               "# NCEP and TOMS filenames\n" +
                                                                               "# if commented or missing, MERIS level1 ancillary data will be used\n" +
                                                                               "# ${ncep_toms}FILE_METEO $file_meteo\n" +
                                                                               "# ${ncep_toms}FILE_OZONE $file_oz\n" +
                                                                               "# possible values ERA_INTERIM or NCEP\n" +
                                                                               "AUXDATA NCEP\n" +
                                                                               "# BANDS DEFINITION #######\n" +
                                                                               "BANDS_CORR 412 443 490 510 560 620 665 754 779 865\n" +
                                                                               "BANDS_OC 412 443 490 510 560 620 665 754 779 865\n" +
                                                                               "BANDS_RW 412 443 490 510 560 620 665 754 779 865\n" +
                                                                               "BANDS_LUTS 412 443 490 510 560 620 665 681 709 754 760 779 865 885 900\n" +
                                                                               "BANDS_L1 412 443 490 510 560 620 665 681 709 754 760 779 865 885 900"));
    }

    @Test
    public void canListAllParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockProcessor.getDefaultSnapBundle()).thenReturn("snap-2.0.0");
        when(mockProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);

        assertThat(calvalusDataInputs.toString(), equalTo("calvalus.calvalus.bundle : calvalus-2.0b411\n" +
                                                          "inputPath : /calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1\n" +
                                                          "autoStaging : true\n" +
                                                          "productionType : L3\n" +
                                                          "calvalus.snap.bundle : snap-2.0.0\n" +
                                                          "processorName : null\n" +
                                                          "processorBundleName : null\n" +
                                                          "processorParameters : null\n" +
                                                          "productionName : dummyProductionName\n" +
                                                          "processorBundleVersion : null\n"));
    }

    @Test(expected = WpsInvalidParameterValueException.class)
    public void canThrowExceptionWhenInputDataSetNameNotPresent() throws Exception {
        Map<String, String> mockInputMapRaw = getRawMapWithoutInputDataSetName();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockProcessor, productSets);
    }

    private String getMockDefaultParameters() {
        return "########################\n" +
               "# POLYMER COMMAND FILE #\n" +
               "########################\n" +
               "# NB: paths can be given as relative or absolute\n" +
               "# lines starting with character '#' are ignored\n" +
               "# INPUT/OUTPUT ######\n" +
               "L1_FORMAT MERIS\n" +
               "# output format: HDF, NETCDF, NETCDF_CLASSIC\n" +
               "OUTPUT_FORMAT NETCDF\n" +
               "# uncomments this to process only a subset of the rows in the L1 file\n" +
               "# PROCESS_ROWS 7700 9000\n" +
               "# NCEP and TOMS filenames\n" +
               "# if commented or missing, MERIS level1 ancillary data will be used\n" +
               "# ${ncep_toms}FILE_METEO $file_meteo\n" +
               "# ${ncep_toms}FILE_OZONE $file_oz\n" +
               "# possible values ERA_INTERIM or NCEP\n" +
               "AUXDATA NCEP\n" +
               "# BANDS DEFINITION #######\n" +
               "BANDS_CORR 412 443 490 510 560 620 665 754 779 865\n" +
               "BANDS_OC 412 443 490 510 560 620 665 754 779 865\n" +
               "BANDS_RW 412 443 490 510 560 620 665 754 779 865\n" +
               "BANDS_LUTS 412 443 490 510 560 620 665 681 709 754 760 779 865 885 900\n" +
               "BANDS_L1 412 443 490 510 560 620 665 681 709 754 760 779 865 885 900";
    }

    private ParameterDescriptor[] getMockParameterDescriptorArray() {
        List<ParameterDescriptor> parameterDescriptors = new ArrayList<>();

        ParameterDescriptor mockParameterDescriptor = mock(ParameterDescriptor.class);
        when(mockParameterDescriptor.getName()).thenReturn("doAtmosphericCorrection");
        parameterDescriptors.add(mockParameterDescriptor);

        ParameterDescriptor mockParameterDescriptor2 = mock(ParameterDescriptor.class);
        when(mockParameterDescriptor2.getName()).thenReturn("doSmileCorrection");
        parameterDescriptors.add(mockParameterDescriptor2);

        ParameterDescriptor mockParameterDescriptor3 = mock(ParameterDescriptor.class);
        when(mockParameterDescriptor3.getName()).thenReturn("outputNormReflec");
        parameterDescriptors.add(mockParameterDescriptor3);

        ParameterDescriptor mockParameterDescriptor4 = mock(ParameterDescriptor.class);
        when(mockParameterDescriptor4.getName()).thenReturn("dummyParameter");
        parameterDescriptors.add(mockParameterDescriptor4);

        return parameterDescriptors.toArray(new ParameterDescriptor[parameterDescriptors.size()]);
    }

    private Map<String, String> getProductionParametersRawMap() {
        Map<String, String> mockInputMapRaw = new HashMap<>();
        mockInputMapRaw.put("inputDataSetName", "MERIS RR  r03 L1b 2002-2012");
        mockInputMapRaw.put("productionType", "L3");
        mockInputMapRaw.put("productionName", "dummyProductionName");
        return mockInputMapRaw;
    }

    private Map<String, String> getProductSetParametersRawMap() {
        Map<String, String> mockInputMapRaw = new HashMap<>();
        mockInputMapRaw.put("inputDataSetName", "MERIS RR  r03 L1b 2002-2012");
        mockInputMapRaw.put("minDate", "2009-06-01");
        mockInputMapRaw.put("maxDate", "2009-06-30");
        mockInputMapRaw.put("periodLength", "30");
        mockInputMapRaw.put("regionWKT", "polygon((10.00 54.00,  14.27 53.47))");
        mockInputMapRaw.put("calvalus.output.format", "NetCDF4");
        return mockInputMapRaw;
    }

    private Map<String, String> getProcessorParametersRawMap() {
        Map<String, String> mockInputMapRaw = new HashMap<>();
        mockInputMapRaw.put("inputDataSetName", "MERIS RR  r03 L1b 2002-2012");
        mockInputMapRaw.put("doAtmosphericCorrection", "true");
        mockInputMapRaw.put("doSmileCorrection", "false");
        mockInputMapRaw.put("outputNormReflec", "true");
        return mockInputMapRaw;
    }

    private Map<String, String> getLessProcessorParametersRawMap() {
        Map<String, String> mockInputMapRaw = new HashMap<>();
        mockInputMapRaw.put("inputDataSetName", "MERIS RR  r03 L1b 2002-2012");
        mockInputMapRaw.put("doAtmosphericCorrection", "true");
        return mockInputMapRaw;
    }

    private Map<String, String> getRawMapWithoutInputDataSetName() {
        Map<String, String> mockInputMapRaw = new HashMap<>();
        mockInputMapRaw.put("inputPath", "/calvalus/eodata/MER_FSG_1P/v2013/${yyyy}/${MM}/${dd}/MER_FSG_1P....${yyyy}${MM}${dd}.*N1$");
        mockInputMapRaw.put("doAtmosphericCorrection", "true");
        mockInputMapRaw.put("doSmileCorrection", "false");
        mockInputMapRaw.put("outputNormReflec", "true");
        return mockInputMapRaw;
    }

    private Map<String, String> getMinimalRawMap() {
        Map<String, String> mockInputMapRaw = new HashMap<>();
        mockInputMapRaw.put("inputDataSetName", "MERIS RR  r03 L1b 2002-2012");
        return mockInputMapRaw;
    }
}