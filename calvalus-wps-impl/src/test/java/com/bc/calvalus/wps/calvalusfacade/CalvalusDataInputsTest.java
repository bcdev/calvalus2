package com.bc.calvalus.wps.calvalusfacade;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.wps.exceptions.WpsInvalidParameterValueException;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
public class CalvalusDataInputsTest {

    private ExecuteRequestExtractor mockExecuteRequestExtractor;
    private CalvalusProcessor mockCalvalusProcessor;
    private ProductSet[] productSets;

    /**
     * Class under test.
     */
    private CalvalusDataInputs calvalusDataInputs;

    @Before
    public void setUp() throws Exception {
        mockExecuteRequestExtractor = mock(ExecuteRequestExtractor.class);
        mockCalvalusProcessor = mock(CalvalusProcessor.class);
        when(mockCalvalusProcessor.getInputProductTypes()).thenReturn(new String[]{"MERIS RR  r03 L1b 2002-2012"});
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
        when(mockCalvalusProcessor.getDefaultBeamBundle()).thenReturn("beam-4.11.1-SNAPSHOT");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.beam.bundle"), equalTo("beam-4.11.1-SNAPSHOT"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionName"), equalTo("dummyProductionName"));
    }

    @Test
    public void canGetProductionParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultBeamBundle()).thenReturn("beam-4.11.1-SNAPSHOT");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getValue("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getValue("calvalus.beam.bundle"), equalTo("beam-4.11.1-SNAPSHOT"));
        assertThat(calvalusDataInputs.getValue("productionName"), equalTo("dummyProductionName"));
    }

    @Test
    public void canGetProcessorInfoParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        when(mockCalvalusProcessor.getBundleName()).thenReturn("beam-idepix");
        when(mockCalvalusProcessor.getBundleVersion()).thenReturn("2.0.9");
        when(mockCalvalusProcessor.getName()).thenReturn("Idepix.Water");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo("beam-idepix"));
        assertThat(calvalusDataInputs.getValue("processorBundleVersion"), equalTo("2.0.9"));
        assertThat(calvalusDataInputs.getValue("processorName"), equalTo("Idepix.Water"));
    }

    @Test
    public void canHandleNoProcessorInfoParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo(null));
        assertThat(calvalusDataInputs.getValue("processorBundleVersion"), equalTo(null));
        assertThat(calvalusDataInputs.getValue("processorName"), equalTo(null));
    }

    @Test
    public void canGetProductSetParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductSetParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

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
        ProcessorDescriptor.ParameterDescriptor[] mockParameterDescriptors = getMockParameterDescriptorArray();
        when(mockCalvalusProcessor.getParameterDescriptors()).thenReturn(mockParameterDescriptors);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

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
        ProcessorDescriptor.ParameterDescriptor[] mockParameterDescriptors = getMockParameterDescriptorArray();
        when(mockCalvalusProcessor.getParameterDescriptors()).thenReturn(mockParameterDescriptors);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo("<parameters>\n" +
                                                                               "<doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
                                                                               "</parameters>"));
    }

    @Test
    public void canGetDefaultProcessorParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultParameters()).thenReturn(getMockDefaultParameters());

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

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
        when(mockCalvalusProcessor.getDefaultBeamBundle()).thenReturn("beam-4.11.1-SNAPSHOT");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

        assertThat(calvalusDataInputs.toString(), equalTo("calvalus.calvalus.bundle : calvalus-2.0b411\n" +
                                                          "inputPath : /calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1\n" +
                                                          "autoStaging : true\n" +
                                                          "productionType : L3\n" +
                                                          "processorName : null\n" +
                                                          "processorBundleName : null\n" +
                                                          "processorParameters : null\n" +
                                                          "productionName : dummyProductionName\n" +
                                                          "calvalus.beam.bundle : beam-4.11.1-SNAPSHOT\n" +
                                                          "processorBundleVersion : null\n"));
    }

    @Test(expected = WpsInvalidParameterValueException.class)
    public void canThrowExceptionWhenInputDataSetNameNotPresent() throws Exception {
        Map<String, String> mockInputMapRaw = getRawMapWithoutInputDataSetName();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);
    }

    @Test
    public void canIgnoreDefaultBeamAndCalvalusBundleVersionWhenNotProvided() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn(null);
        when(mockCalvalusProcessor.getDefaultBeamBundle()).thenReturn(null);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets);

        assertThat(calvalusDataInputs.toString(), equalTo("inputPath : /calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1\n" +
                                                          "autoStaging : true\n" +
                                                          "processorName : null\n" +
                                                          "processorBundleName : null\n" +
                                                          "processorParameters : null\n" +
                                                          "processorBundleVersion : null\n"));
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

    private ProcessorDescriptor.ParameterDescriptor[] getMockParameterDescriptorArray() {
        List<ProcessorDescriptor.ParameterDescriptor> parameterDescriptors = new ArrayList<>();

        ProcessorDescriptor.ParameterDescriptor mockParameterDescriptor = mock(ProcessorDescriptor.ParameterDescriptor.class);
        when(mockParameterDescriptor.getName()).thenReturn("doAtmosphericCorrection");
        parameterDescriptors.add(mockParameterDescriptor);

        ProcessorDescriptor.ParameterDescriptor mockParameterDescriptor2 = mock(ProcessorDescriptor.ParameterDescriptor.class);
        when(mockParameterDescriptor2.getName()).thenReturn("doSmileCorrection");
        parameterDescriptors.add(mockParameterDescriptor2);

        ProcessorDescriptor.ParameterDescriptor mockParameterDescriptor3 = mock(ProcessorDescriptor.ParameterDescriptor.class);
        when(mockParameterDescriptor3.getName()).thenReturn("outputNormReflec");
        parameterDescriptors.add(mockParameterDescriptor3);

        ProcessorDescriptor.ParameterDescriptor mockParameterDescriptor4 = mock(ProcessorDescriptor.ParameterDescriptor.class);
        when(mockParameterDescriptor4.getName()).thenReturn("dummyParameter");
        parameterDescriptors.add(mockParameterDescriptor4);

        return parameterDescriptors.toArray(new ProcessorDescriptor.ParameterDescriptor[parameterDescriptors.size()]);
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