package com.bc.calvalus.wps.calvalusfacade;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
public class CalvalusDataInputsTest {

    private static final String DUMMY_REMOTE_USER = "dummyRemoteUser";
    private ExecuteRequestExtractor mockExecuteRequestExtractor;
    private CalvalusProcessor mockCalvalusProcessor;
    private ProductSet[] productSets;

    /**
     * Class under test.
     */
    private CalvalusDataInputs calvalusDataInputs;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        mockExecuteRequestExtractor = mock(ExecuteRequestExtractor.class);
        mockCalvalusProcessor = mock(CalvalusProcessor.class);
        when(mockCalvalusProcessor.getInputProductTypes()).thenReturn(new String[]{"MERIS RR  r03 L1b 2002-2012"});
        productSets = getMockProductSets();
    }

    @Test
    public void canGetInputMapFormatted() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn("snap-3.0.0");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.snap.bundle"), equalTo("snap-3.0.0"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionName"), equalTo("dummyProductionName"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("minDateSource"), equalTo("2016-01-01"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("maxDateSource"), equalTo("2017-01-01"));
    }

    @Test
    public void canGetInputMapFormattedWithProductionSetDateRange() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn("snap-3.0.0");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");
        ProductSet[] productSetsWithDateRange = getMockProductSetsWithDates();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSetsWithDateRange, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.snap.bundle"), equalTo("snap-3.0.0"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionName"), equalTo("dummyProductionName"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("minDateSource"), equalTo("2000-01-01"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("maxDateSource"), equalTo("2010-01-01"));
    }

    @Test
    public void canGetInputMapFormattedWithProductionSetGeoDb() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn("snap-3.0.0");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.10-SNAPSHOT");
        ProductSet[] productSetsWithGeoDb = getMockProductSetsWithGeoDb();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSetsWithGeoDb, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.calvalus.bundle"), equalTo("calvalus-2.10-SNAPSHOT"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.snap.bundle"), equalTo("snap-3.0.0"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionName"), equalTo("dummyProductionName"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("minDateSource"), equalTo("2016-01-01"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("maxDateSource"), equalTo("2017-01-01"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("geoInventory"), equalTo("/calvalus/geoInventory/URBAN_FOOTPRINT_GUF_GLOBAL_75m"));
    }

    @Test
    public void canGetProductionParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn("snap-3.0.0");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getValue("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getValue("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getValue("calvalus.snap.bundle"), equalTo("snap-3.0.0"));
        assertThat(calvalusDataInputs.getValue("productionName"), equalTo("dummyProductionName"));
    }

    @Test
    public void canGetProcessorInfoParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        when(mockCalvalusProcessor.getBundleName()).thenReturn("beam-idepix");
        when(mockCalvalusProcessor.getBundleVersion()).thenReturn("2.0.9");
        when(mockCalvalusProcessor.getName()).thenReturn("Idepix.Water");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo("beam-idepix"));
        assertThat(calvalusDataInputs.getValue("processorBundleVersion"), equalTo("2.0.9"));
        assertThat(calvalusDataInputs.getValue("processorName"), equalTo("Idepix.Water"));
    }

    @Test
    public void canHandleNoProcessorInfoParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo(null));
        assertThat(calvalusDataInputs.getValue("processorBundleVersion"), equalTo(null));
        assertThat(calvalusDataInputs.getValue("processorName"), equalTo(null));
    }

    @Test
    public void canGetProductSetParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductSetParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getValue("inputPath"), equalTo("/calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1"));
        assertThat(calvalusDataInputs.getValue("minDate"), equalTo("2009-06-01"));
        assertThat(calvalusDataInputs.getValue("maxDate"), equalTo("2009-06-30"));
        assertThat(calvalusDataInputs.getValue("periodLength"), equalTo("30"));
        assertThat(calvalusDataInputs.getValue("regionWKT"), equalTo("polygon((10.00 54.00,  14.27 53.47))"));
        assertThat(calvalusDataInputs.getValue("outputFormat"), equalTo("NetCDF4"));
    }

    @Test
    public void canGetMultiPatternProductSetParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductSetParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        ProductSet[] productSetsMultiPattern = getMockProductSetsWithMultiPatterns();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSetsMultiPattern, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getValue("inputPath"), equalTo("/calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1," +
                                                                     "/calvalus/eodata/MER_RRG__1P/r03/${yyyy}/${MM}/${dd}/.*.N1," +
                                                                     "/calvalus/eodata/MER_FRS__1P/r03/${yyyy}/${MM}/${dd}/.*.N1"));
        assertThat(calvalusDataInputs.getValue("minDate"), equalTo("2009-06-01"));
        assertThat(calvalusDataInputs.getValue("maxDate"), equalTo("2009-06-30"));
        assertThat(calvalusDataInputs.getValue("periodLength"), equalTo("30"));
        assertThat(calvalusDataInputs.getValue("regionWKT"), equalTo("polygon((10.00 54.00,  14.27 53.47))"));
        assertThat(calvalusDataInputs.getValue("outputFormat"), equalTo("NetCDF4"));

    }

    @Test
    public void canTransformProcessorParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProcessorParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        ParameterDescriptor[] mockParameterDescriptors = getMockParameterDescriptorArray();
        when(mockCalvalusProcessor.getParameterDescriptors()).thenReturn(mockParameterDescriptors);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

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
        when(mockCalvalusProcessor.getParameterDescriptors()).thenReturn(mockParameterDescriptors);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo("<parameters>\n" +
                                                                               "<doAtmosphericCorrection>true</doAtmosphericCorrection>\n" +
                                                                               "<doSmileCorrection>false</doSmileCorrection>\n" +
                                                                               "</parameters>"));
    }

    @Test
    public void canGetDefaultProcessorParametersWhenNotProvided() throws Exception {
        Map<String, String> mockInputMapRaw = getNoProcessorParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        ParameterDescriptor[] mockParameterDescriptors = getMockParameterDescriptorArray();
        when(mockCalvalusProcessor.getParameterDescriptors()).thenReturn(mockParameterDescriptors);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo("<parameters>\n" +
                                                                               "<doSmileCorrection>false</doSmileCorrection>\n" +
                                                                               "</parameters>"));
    }

    @Test
    public void canGetDefaultProcessorParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultParameters()).thenReturn(getMockDefaultParameters());

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

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
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn("snap-3.0.0");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");
        when(mockCalvalusProcessor.getBundleLocation()).thenReturn("hdfs://calvalus/calvalus/software/1.0/beam-buildin-1.0");

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.toString(), equalTo("processorBundleLocation : hdfs://calvalus/calvalus/software/1.0/beam-buildin-1.0\n" +
                                                          "calvalus.wps.remote.user : dummyRemoteUser\n" +
                                                          "productionType : L3\n" +
                                                          "inputDataSetName : MERIS RR  r03 L1b 2002-2012\n" +
                                                          "processorBundleVersion : null\n" +
                                                          "calvalus.calvalus.bundle : calvalus-2.0b411\n" +
                                                          "inputPath : /calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1\n" +
                                                          "minDateSource : 2016-01-01\n" +
                                                          "autoStaging : true\n" +
                                                          "calvalus.snap.bundle : snap-3.0.0\nprocessorName : null\n" +
                                                          "processorBundleName : null\n" +
                                                          "processorParameters : null\n" +
                                                          "productionName : dummyProductionName\n" +
                                                          "maxDateSource : 2017-01-01\n"));
    }

    @Test(expected = InvalidParameterValueException.class)
    public void canThrowExceptionWhenInputDataSetNameNotPresent() throws Exception {
        Map<String, String> mockInputMapRaw = getRawMapWithoutInputDataSetName();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);
    }

    @Test
    public void canIgnoreDefaultBeamAndCalvalusBundleVersionWhenNotProvided() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn(null);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn(null);

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, DUMMY_REMOTE_USER);

        assertThat(calvalusDataInputs.toString(), equalTo("inputPath : /calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1\n" +
                                                          "processorBundleLocation : null\n" +
                                                          "calvalus.wps.remote.user : dummyRemoteUser\n" +
                                                          "minDateSource : 2016-01-01\n" +
                                                          "autoStaging : true\n" +
                                                          "processorName : null\n" +
                                                          "processorBundleName : null\n" +
                                                          "processorParameters : null\n" +
                                                          "inputDataSetName : MERIS RR  r03 L1b 2002-2012\n" +
                                                          "processorBundleVersion : null\n" +
                                                          "maxDateSource : 2017-01-01\n"));
    }

    private ProductSet[] getMockProductSets() {
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getName()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        when(mockProductSet.getPath()).thenReturn("eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1");
        when(mockProductSet.getProductType()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        return new ProductSet[]{mockProductSet};
    }

    private ProductSet[] getMockProductSetsWithMultiPatterns() {
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getName()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        when(mockProductSet.getPath()).thenReturn("eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1," +
                                                  "eodata/MER_RRG__1P/r03/${yyyy}/${MM}/${dd}/.*.N1," +
                                                  "eodata/MER_FRS__1P/r03/${yyyy}/${MM}/${dd}/.*.N1");
        when(mockProductSet.getProductType()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        return new ProductSet[]{mockProductSet};
    }

    private ProductSet[] getMockProductSetsWithGeoDb() {
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getName()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        when(mockProductSet.getGeoInventory()).thenReturn("/calvalus/geoInventory/URBAN_FOOTPRINT_GUF_GLOBAL_75m");
        when(mockProductSet.getPath()).thenReturn("eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1");
        when(mockProductSet.getProductType()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        return new ProductSet[]{mockProductSet};
    }

    private ProductSet[] getMockProductSetsWithDates() {
        ProductSet mockProductSet = mock(ProductSet.class);
        when(mockProductSet.getName()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        when(mockProductSet.getPath()).thenReturn("eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1");
        when(mockProductSet.getProductType()).thenReturn("MERIS RR  r03 L1b 2002-2012");
        when(mockProductSet.getMinDate()).thenReturn(new Date(946684800000L));
        when(mockProductSet.getMaxDate()).thenReturn(new Date(1262304000000L));
        return new ProductSet[]{mockProductSet};
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
        when(mockParameterDescriptor2.getDefaultValue()).thenReturn("false");
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
        mockInputMapRaw.put("outputFormat", "NetCDF4");
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

    private Map<String, String> getNoProcessorParametersRawMap() {
        Map<String, String> mockInputMapRaw = new HashMap<>();
        mockInputMapRaw.put("inputDataSetName", "MERIS RR  r03 L1b 2002-2012");
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