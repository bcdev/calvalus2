package com.bc.calvalus.wps.calvalusfacade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.utils.ProcessorNameParser;
import org.junit.*;

import java.util.List;

/**
 * @author hans
 */
public class CalvalusProcessorExtractorTest {

    private CalvalusProcessorExtractor processorExtractor;

    private ProductionService mockProductionService;

    @Before
    public void setUp() throws Exception {
        mockProductionService = mock(ProductionService.class);
    }

    @Test
    public void canGetProcessors() throws Exception {
        ProcessorDescriptor[] mockProcessorDescriptors = getMockProcessorDescriptors();
        BundleDescriptor[] mockBundleDescriptors = getMockBundleDescriptors(mockProcessorDescriptors);
        when(mockProductionService.getBundles(anyString(), any(BundleFilter.class))).thenReturn(mockBundleDescriptors);

        processorExtractor = new CalvalusProcessorExtractor();
        List<IWpsProcess> processors = processorExtractor.getProcessors(mockProductionService, "mockUserName");

        assertThat(processors.size(), equalTo(1));
        assertThat(processors.get(0).getTitle(), equalTo("Urban TEP indices Meris L1b"));
        assertThat(processors.get(0).getIdentifier(), equalTo("beam-buildin~1.0~urban-tep-indices-meris-l1b"));
        assertThat(processors.get(0).getVersion(), equalTo("0.1"));
    }

    @Test
    public void canGetEmptyProcessorListWhenNoProcessorDescriptor() throws Exception {
        BundleDescriptor mockBundleDescriptor = mock(BundleDescriptor.class);
        when(mockBundleDescriptor.getProcessorDescriptors()).thenReturn(null);
        BundleDescriptor[] mockBundleDescriptors = new BundleDescriptor[]{mockBundleDescriptor};
        when(mockProductionService.getBundles(anyString(), any(BundleFilter.class))).thenReturn(mockBundleDescriptors);

        processorExtractor = new CalvalusProcessorExtractor();
        List<IWpsProcess> processors = processorExtractor.getProcessors(mockProductionService, "mockUserName");

        assertThat(processors.size(), equalTo(0));
    }

    @Test
    public void canGetEmptyProcessorListWhenNoBundleDescriptor() throws Exception {
        BundleDescriptor[] mockBundleDescriptors = new BundleDescriptor[]{};
        when(mockProductionService.getBundles(anyString(), any(BundleFilter.class))).thenReturn(mockBundleDescriptors);

        processorExtractor = new CalvalusProcessorExtractor();
        List<IWpsProcess> processors = processorExtractor.getProcessors(mockProductionService, "mockUserName");

        assertThat(processors.size(), equalTo(0));
    }

    @Test
    public void canGetSingleProcessor() throws Exception {
        ProcessorDescriptor[] mockProcessorDescriptors = getMockProcessorDescriptors();
        BundleDescriptor[] mockBundleDescriptors = getMockBundleDescriptors(mockProcessorDescriptors);
        when(mockProductionService.getBundles(anyString(), any(BundleFilter.class))).thenReturn(mockBundleDescriptors);
        ProcessorNameParser mockParser = getMatchingParser();

        processorExtractor = new CalvalusProcessorExtractor();
        CalvalusProcessor processor = processorExtractor.getProcessor(mockParser, mockProductionService, "mockUserName");

        assertThat(processor.getTitle(), equalTo("Urban TEP indices Meris L1b"));
        assertThat(processor.getIdentifier(), equalTo("beam-buildin~1.0~urban-tep-indices-meris-l1b"));
        assertThat(processor.getVersion(), equalTo("0.1"));
    }

    @Test
    public void canReturnNullWhenNoMatchingProcessorDescriptor() throws Exception {
        ProcessorDescriptor[] mockProcessorDescriptors = getMockProcessorDescriptors();
        BundleDescriptor[] mockBundleDescriptors = getMockBundleDescriptors(mockProcessorDescriptors);
        when(mockProductionService.getBundles(anyString(), any(BundleFilter.class))).thenReturn(mockBundleDescriptors);
        ProcessorNameParser mockParser = getUnmatchingParser();

        processorExtractor = new CalvalusProcessorExtractor();
        CalvalusProcessor processors = processorExtractor.getProcessor(mockParser, mockProductionService, "mockUserName");

        assertThat(processors, equalTo(null));
    }

    @Test
    public void canReturnNullWhenNoBundleDescriptor() throws Exception {
        BundleDescriptor[] mockBundleDescriptors = new BundleDescriptor[]{};
        when(mockProductionService.getBundles(anyString(), any(BundleFilter.class))).thenReturn(mockBundleDescriptors);
        ProcessorNameParser mockParser = getMatchingParser();

        processorExtractor = new CalvalusProcessorExtractor();
        CalvalusProcessor processors = processorExtractor.getProcessor(mockParser, mockProductionService, "mockUserName");

        assertThat(processors, equalTo(null));
    }

    @Test(expected = ProductionException.class)
    public void canThrowProductionException() throws Exception {
        when(mockProductionService.getBundles(anyString(), any(BundleFilter.class))).thenThrow(new ProductionException("production exception"));
        ProcessorNameParser mockParser = getMatchingParser();

        processorExtractor = new CalvalusProcessorExtractor();
        processorExtractor.getProcessor(mockParser, mockProductionService, "mockUserName");
    }

    private ProcessorNameParser getMatchingParser() {
        ProcessorNameParser mockParser = mock(ProcessorNameParser.class);
        when(mockParser.getBundleName()).thenReturn("beam-buildin");
        when(mockParser.getBundleVersion()).thenReturn("1.0");
        when(mockParser.getExecutableName()).thenReturn("urban-tep-indices-meris-l1b");
        return mockParser;
    }

    private ProcessorNameParser getUnmatchingParser() {
        ProcessorNameParser mockParser = mock(ProcessorNameParser.class);
        when(mockParser.getBundleName()).thenReturn("random-bundle");
        when(mockParser.getBundleVersion()).thenReturn("0.0");
        when(mockParser.getExecutableName()).thenReturn("random-processor");
        return mockParser;
    }

    private BundleDescriptor[] getMockBundleDescriptors(ProcessorDescriptor[] mockProcessorDescriptors) {
        BundleDescriptor mockBundleDescriptor = mock(BundleDescriptor.class);
        when(mockBundleDescriptor.getProcessorDescriptors()).thenReturn(mockProcessorDescriptors);
        when(mockBundleDescriptor.getBundleName()).thenReturn("beam-buildin");
        when(mockBundleDescriptor.getBundleVersion()).thenReturn("1.0");
        return new BundleDescriptor[]{mockBundleDescriptor};
    }

    private ProcessorDescriptor[] getMockProcessorDescriptors() {
        ProcessorDescriptor mockProcessorDescriptor = mock(ProcessorDescriptor.class);
        when(mockProcessorDescriptor.getProcessorVersion()).thenReturn("0.1");
        when(mockProcessorDescriptor.getProcessorName()).thenReturn("Urban TEP indices Meris L1b");
        when(mockProcessorDescriptor.getExecutableName()).thenReturn("urban-tep-indices-meris-l1b");
        return new ProcessorDescriptor[]{mockProcessorDescriptor};
    }

}