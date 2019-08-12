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
    private static final String DUMMY_REMOTE_REF = "1738ad7b-534e-4aca-9861-b26fb9c0f983";
    private static final String[][] FEATURES = new String[][] {
        new String[] { "attr1", "attr2", "attr3" },
        new String[] { "value1", "value1", "value3" },
        new String[] { "value1", "value2", "value3" },
        new String[] { "value1", "value3", "value2" }
    };
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
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

        //assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L2Plus"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.snap.bundle"), equalTo("snap-3.0.0"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionName"), equalTo("dummyProductionName"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("minDateSource"), equalTo("2016-01-01T01:00:00+01:00"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("maxDateSource"), equalTo("2017-01-01T01:00:00+01:00"));
    }

    @Test
    public void canGetInputMapFormattedWithProductionSetDateRange() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn("snap-3.0.0");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");
        ProductSet[] productSetsWithDateRange = getMockProductSetsWithDates();
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSetsWithDateRange, mockCalvalusFacade);

//        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L2Plus"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.calvalus.bundle"), equalTo("calvalus-2.0b411"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.snap.bundle"), equalTo("snap-3.0.0"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionName"), equalTo("dummyProductionName"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("minDateSource"), equalTo("2000-01-01T01:00:00+01:00"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("maxDateSource"), equalTo("2010-01-01T01:00:00+01:00"));
    }

    @Test
    public void canGetInputMapFormattedWithProductionSetGeoDb() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn("snap-3.0.0");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.10-SNAPSHOT");
        ProductSet[] productSetsWithGeoDb = getMockProductSetsWithGeoDb();
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSetsWithGeoDb, mockCalvalusFacade);

//        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionType"), equalTo("L2Plus"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.calvalus.bundle"), equalTo("calvalus-2.10-SNAPSHOT"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("calvalus.snap.bundle"), equalTo("snap-3.0.0"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("productionName"), equalTo("dummyProductionName"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("minDateSource"), equalTo("2016-01-01T01:00:00+01:00"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("maxDateSource"), equalTo("2017-01-01T01:00:00+01:00"));
        assertThat(calvalusDataInputs.getInputMapFormatted().get("geoInventory"), equalTo("/calvalus/geoInventory/URBAN_FOOTPRINT_GUF_GLOBAL_75m"));
    }

    @Test
    public void canGetProductionParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductionParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn("snap-3.0.0");
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn("calvalus-2.0b411");
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

//        assertThat(calvalusDataInputs.getValue("productionType"), equalTo("L3"));
        assertThat(calvalusDataInputs.getValue("productionType"), equalTo("L2Plus"));
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
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo("beam-idepix"));
        assertThat(calvalusDataInputs.getValue("processorBundleVersion"), equalTo("2.0.9"));
        assertThat(calvalusDataInputs.getValue("processorName"), equalTo("Idepix.Water"));
    }

    @Test
    public void canHandleNoProcessorInfoParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

        assertThat(calvalusDataInputs.getValue("processorBundleName"), equalTo(null));
        assertThat(calvalusDataInputs.getValue("processorBundleVersion"), equalTo(null));
        assertThat(calvalusDataInputs.getValue("processorName"), equalTo(null));
    }

    @Test
    public void canGetProductSetParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getProductSetParametersRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

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
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSetsMultiPattern, mockCalvalusFacade);

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
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

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
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

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
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

        assertThat(calvalusDataInputs.getValue("processorParameters"), equalTo("<parameters>\n" +
                                                                                       "<doSmileCorrection>false</doSmileCorrection>\n" +
                                                                                       "</parameters>"));
    }

    @Test
    public void canGetDefaultProcessorParameters() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultParameters()).thenReturn(getMockDefaultParameters());
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

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
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

        assertThat(calvalusDataInputs.toString(),
                   equalTo("calvalus.wps.remote.ref : 1738ad7b-534e-4aca-9861-b26fb9c0f983\n" +
                                   "minDate : 2016-01-01T01:00:00+01:00\n" +
                                   "calvalus.output.compression : none\n" +
                                   "processorBundleLocation : hdfs://calvalus/calvalus/software/1.0/beam-buildin-1.0\n" +
                                   "calvalus.wps.remote.user : dummyRemoteUser\n" +
//                                                          "productionType : L3\n" +
                                   "productionType : L2Plus\n" +
                                   "inputDataSetName : MERIS RR  r03 L1b 2002-2012\n" +
                                   "processorBundleVersion : null\n" +
                                   "calvalus.calvalus.bundle : calvalus-2.0b411\n" +
                                   "inputPath : /calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1\n" +
                                   "minDateSource : 2016-01-01T01:00:00+01:00\n" +
                                   "quicklooks : true\n" +
                                   "autoStaging : true\n" +
                                   "calvalus.snap.bundle : snap-3.0.0\n" +
                                   "processorName : null\n" +
                                   "maxDate : 2017-01-01T01:00:00+01:00\n" +
                                   "processorBundleName : null\n" +
                                   "processorParameters : null\n" +
                                   "productionName : dummyProductionName\n" +
                                   "maxDateSource : 2017-01-01T01:00:00+01:00\n" +
                                   "calvalus.system.snap.dataio.bigtiff.support.pushprocessing : false\n"));
    }

    @Test(expected = InvalidParameterValueException.class)
    public void canThrowExceptionWhenInputDataSetNameNotPresent() throws Exception {
        Map<String, String> mockInputMapRaw = getRawMapWithoutInputDataSetName();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);
    }

    @Test
    public void canIgnoreDefaultBeamAndCalvalusBundleVersionWhenNotProvided() throws Exception {
        Map<String, String> mockInputMapRaw = getMinimalRawMap();
        when(mockExecuteRequestExtractor.getInputParametersMapRaw()).thenReturn(mockInputMapRaw);
        when(mockCalvalusProcessor.getDefaultCalvalusBundle()).thenReturn(null);
        when(mockCalvalusProcessor.getDefaultSnapBundle()).thenReturn(null);
        CalvalusFacade mockCalvalusFacade = getMockCalvalusFacade();

        calvalusDataInputs = new CalvalusDataInputs(mockExecuteRequestExtractor, mockCalvalusProcessor, productSets, mockCalvalusFacade);

        assertThat(calvalusDataInputs.toString(),
                   equalTo("calvalus.wps.remote.ref : 1738ad7b-534e-4aca-9861-b26fb9c0f983\n" +
                                   "minDate : 2016-01-01T01:00:00+01:00\n" +
                                   "calvalus.output.compression : none\n" +
                                   "processorBundleLocation : null\n" +
                                   "calvalus.wps.remote.user : dummyRemoteUser\n" +
                                   "productionType : L2Plus\n" +
                                   "inputDataSetName : MERIS RR  r03 L1b 2002-2012\n" +
                                   "processorBundleVersion : null\n" +
                                   "inputPath : /calvalus/eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1\n" +
                                   "minDateSource : 2016-01-01T01:00:00+01:00\n" +
                                   "quicklooks : true\n" +
                                   "autoStaging : true\n" +
                                   "processorName : null\n" +
                                   "maxDate : 2017-01-01T01:00:00+01:00\n" +
                                   "processorBundleName : null\n" +
                                   "processorParameters : null\n" +
                                   "maxDateSource : 2017-01-01T01:00:00+01:00\n" +
                                   "calvalus.system.snap.dataio.bigtiff.support.pushprocessing : false\n"));
    }

    @Test
    public void testDistinctiveAttributes() throws InvalidParameterValueException {
        assertThat("attr2", equalTo(CalvalusDataInputs.determineMostDistinctiveAttribute(FEATURES)));
    }

    private CalvalusFacade getMockCalvalusFacade() {
        CalvalusFacade calvalusFacade = mock(CalvalusFacade.class);
        when(calvalusFacade.getRequestHeaderMap()).thenReturn(getMockRequestHeader());
        return calvalusFacade;
    }

    private Map<String, String> getMockRequestHeader() {
        Map<String, String> mockRequestHeader = new HashMap<>();
        mockRequestHeader.put("remoteUser", DUMMY_REMOTE_USER);
        mockRequestHeader.put("remoteRef", DUMMY_REMOTE_REF);
        return mockRequestHeader;
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
        mockInputMapRaw.put("calvalus", "L3");
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