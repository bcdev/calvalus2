package com.bc.calvalus.wpsrest;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
public class ProcessorTest {

    /**
     * Class under test.
     */
    private Processor processor;

    private BundleDescriptor mockBundleDescriptor;
    private ProcessorDescriptor mockProcessorDescriptor;

    @Before
    public void setUp() throws Exception {
        mockBundleDescriptor = mock(BundleDescriptor.class);
        mockProcessorDescriptor = mock(ProcessorDescriptor.class);
    }

    @Test
    public void canGetIdentifier() throws Exception {
        when(mockBundleDescriptor.getBundleName()).thenReturn("beam-idepix");
        when(mockBundleDescriptor.getBundleVersion()).thenReturn("1.0.0");
        when(mockProcessorDescriptor.getExecutableName()).thenReturn("Idepix.Water");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getIdentifier(), equalTo("beam-idepix~1.0.0~Idepix.Water"));
    }

    @Test
    public void canGetTitle() throws Exception {
        when(mockProcessorDescriptor.getProcessorName()).thenReturn("dummyProcessorName");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getTitle(), equalTo("dummyProcessorName"));
    }

    @Test
    public void canGetAbstractText() throws Exception {
        when(mockProcessorDescriptor.getDescriptionHtml()).thenReturn("dummyProcessorDescription");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getAbstractText(), equalTo("dummyProcessorDescription"));
    }

    @Test
    public void canGetParameterDescriptors() throws Exception {
        ParameterDescriptor[] mockParameterDescriptors = getParameterDescriptors();
        when(mockProcessorDescriptor.getParameterDescriptors()).thenReturn(mockParameterDescriptors);

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        ParameterDescriptor[] parameterDescriptors = processor.getParameterDescriptors();
        ParameterDescriptor parameter1 = parameterDescriptors[0];
        ParameterDescriptor parameter2 = parameterDescriptors[1];

        assertThat(parameter1.getName(), equalTo("ccOutputRadiance"));
        assertThat(parameter1.getDescription(), equalTo("Whether to additionally write TOA Radiances to the target product"));
        assertThat(parameter1.getType(), equalTo("boolean"));
        assertThat(parameter1.getDefaultValue(), equalTo("true"));

        assertThat(parameter2.getName(), equalTo("ccCloudBufferWidth"));
        assertThat(parameter2.getDescription(), equalTo("The width (# of pixels) of the 'safety buffer' around a pixel identified " +
                                                        "as cloudy, must be in [0,100]"));
        assertThat(parameter2.getType(), equalTo("string"));
        assertThat(parameter2.getDefaultValue(), equalTo("2"));
    }

    @Test
    public void canGetDefaultParameters() throws Exception {
        when(mockProcessorDescriptor.getDefaultParameters()).thenReturn(getSampleProcessorParameterTxt());

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getDefaultParameters(), equalTo("########################\n" +
                                                             "# POLYMER COMMAND FILE #\n" +
                                                             "########################\n" +
                                                             "# NB: paths can be given as relative or absolute\n" +
                                                             "# lines starting with character '#' are ignored\n" +
                                                             "# INPUT/OUTPUT ######\nL1_FORMAT SEAWIFS\n" +
                                                             "# output format: HDF, NETCDF, NETCDF_CLASSIC\n"));
    }

    @Test
    public void canGetDefaultCalvalusBundle() throws Exception {
        when(mockProcessorDescriptor.getJobConfiguration()).thenReturn(getJobConfiguration());

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getDefaultCalvalusBundle(), equalTo("calvalus-2.7-SNAPSHOT"));
    }

    @Test
    public void canGetDefaultBeamBundle() throws Exception {
        when(mockProcessorDescriptor.getJobConfiguration()).thenReturn(getJobConfiguration());

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getDefaultBeamBundle(), equalTo("beam-5.0.1"));
    }

    @Test
    public void canGetBundleName() throws Exception {
        when(mockBundleDescriptor.getBundleName()).thenReturn("dummyBundleName");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getBundleName(), equalTo("dummyBundleName"));
    }

    @Test
    public void canGetBundleVersion() throws Exception {
        when(mockBundleDescriptor.getBundleVersion()).thenReturn("dummyBundleVersion");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getBundleVersion(), equalTo("dummyBundleVersion"));
    }

    @Test
    public void canGetName() throws Exception {
        when(mockProcessorDescriptor.getExecutableName()).thenReturn("Idepix.Water");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getName(), equalTo("Idepix.Water"));
    }

    @Test
    public void canGetVersion() throws Exception {
        when(mockProcessorDescriptor.getProcessorVersion()).thenReturn("1.0.0");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getVersion(), equalTo("1.0.0"));
    }

    @Test
    public void canGetInputProductTypes() throws Exception {
        String[] mockInputProductTypes = getInputProductTypes();
        when(mockProcessorDescriptor.getInputProductTypes()).thenReturn(mockInputProductTypes);

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        String[] inputProductTypes = processor.getInputProductTypes();
        List<String> inputProductTypeList = Arrays.asList(inputProductTypes);

        assertThat(inputProductTypeList, hasItem("MERIS RR  r03 L1b 2002-2012"));
        assertThat(inputProductTypeList, hasItem("MERIS RRG r03 L1b 2002-2012"));
        assertThat(inputProductTypeList, hasItem("MERIS FSG v2013 L1b 2002-2012"));
    }

    private String getSampleProcessorParameterTxt() {
        return "########################\n" +
               "# POLYMER COMMAND FILE #\n" +
               "########################\n" +
               "# NB: paths can be given as relative or absolute\n" +
               "# lines starting with character '#' are ignored\n" +
               "# INPUT/OUTPUT ######\n" +
               "L1_FORMAT SEAWIFS\n" +
               "# output format: HDF, NETCDF, NETCDF_CLASSIC\n";
    }

    private Map<String, String> getJobConfiguration() {
        Map<String, String> jobConfiguration = new HashMap<>();
        jobConfiguration.put("calvalus.calvalus.bundle", "calvalus-2.7-SNAPSHOT");
        jobConfiguration.put("calvalus.beam.bundle", "beam-5.0.1");
        return jobConfiguration;
    }

    private ParameterDescriptor[] getParameterDescriptors() {
        List<ParameterDescriptor> parameterDescriptorList = new ArrayList<>();
        ParameterDescriptor mockParameterDescriptor1 = mock(ParameterDescriptor.class);
        when(mockParameterDescriptor1.getName()).thenReturn("ccOutputRadiance");
        when(mockParameterDescriptor1.getDescription()).thenReturn("Whether to additionally write TOA Radiances to the target product");
        when(mockParameterDescriptor1.getDefaultValue()).thenReturn("true");
        when(mockParameterDescriptor1.getType()).thenReturn("boolean");
        parameterDescriptorList.add(mockParameterDescriptor1);

        ParameterDescriptor mockParameterDescriptor2 = mock(ParameterDescriptor.class);
        when(mockParameterDescriptor2.getName()).thenReturn("ccCloudBufferWidth");
        when(mockParameterDescriptor2.getDescription()).thenReturn("The width (# of pixels) of the 'safety buffer' around a pixel identified as cloudy, must be in [0,100]");
        when(mockParameterDescriptor2.getDefaultValue()).thenReturn("2");
        when(mockParameterDescriptor2.getType()).thenReturn("string");
        parameterDescriptorList.add(mockParameterDescriptor2);

        return parameterDescriptorList.toArray(new ParameterDescriptor[parameterDescriptorList.size()]);
    }

    private String[] getInputProductTypes() {
        List<String> inputProductTypeList = new ArrayList<>();
        inputProductTypeList.add("MERIS RR  r03 L1b 2002-2012");
        inputProductTypeList.add("MERIS RRG r03 L1b 2002-2012");
        inputProductTypeList.add("MERIS FSG v2013 L1b 2002-2012");
        return inputProductTypeList.toArray(new String[inputProductTypeList.size()]);
    }
}