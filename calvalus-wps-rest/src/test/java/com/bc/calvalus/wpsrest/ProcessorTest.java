package com.bc.calvalus.wpsrest;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import org.junit.*;

/**
 * Created by hans on 08/09/2015.
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
    public void testGetParameterDescriptors() throws Exception {

    }

    @Test
    public void testGetDefaultParameters() throws Exception {
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
    public void testGetDefaultCalvalusBundle() throws Exception {

    }

    @Test
    public void testGetDefaultBeamBundle() throws Exception {

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
    public void testGetName() throws Exception {
        when(mockProcessorDescriptor.getExecutableName()).thenReturn("Idepix.Water");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getName(), equalTo("Idepix.Water"));
    }

    @Test
    public void testGetVersion() throws Exception {
        when(mockProcessorDescriptor.getProcessorVersion()).thenReturn("1.0.0");

        processor = new Processor(mockBundleDescriptor, mockProcessorDescriptor);

        assertThat(processor.getVersion(), equalTo("1.0.0"));
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
}