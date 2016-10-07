package com.bc.calvalus.wps.localprocess;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.utilities.PropertiesWrapper;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LocalProcessorExtractor.class, IOUtils.class, PropertiesWrapper.class})
public class LocalProcessorExtractorTest {

    private static final String DUMMY_USER_NAME = "dummyName";

    private LocalProcessorExtractor processorExtractor;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
    }

    @Test
    public void canGetProcessors() throws Exception {
        processorExtractor = new LocalProcessorExtractor();
        List<WpsProcess> processors = processorExtractor.getProcessors(DUMMY_USER_NAME);

        assertThat(processors.size(), equalTo(1));
        assertThat(processors.get(0).getTitle(), equalTo("Urban TEP local subsetting for test"));
        assertThat(processors.get(0).getIdentifier(), equalTo("urbantep-local~1.0~Subset"));
        assertThat(processors.get(0).getVersion(), equalTo("1.0"));
    }

    @Test
    public void canGetSingleProcessor() throws Exception {
        ProcessorNameConverter mockParser = getMatchingParser();

        processorExtractor = new LocalProcessorExtractor();
        WpsProcess processor = processorExtractor.getProcessor(mockParser, "mockUserName");

        assertThat(processor.getTitle(), equalTo("Urban TEP local subsetting for test"));
        assertThat(processor.getIdentifier(), equalTo("urbantep-local~1.0~Subset"));
        assertThat(processor.getVersion(), equalTo("1.0"));
    }

    @Test
    public void canReturnNullWhenNoMatchingProcessorDescriptor() throws Exception {
        ProcessorNameConverter mockParser = getUnmatchingParser();

        processorExtractor = new LocalProcessorExtractor();
        WpsProcess processors = processorExtractor.getProcessor(mockParser, "mockUserName");

        assertThat(processors, equalTo(null));
    }

    @Test(expected = WpsProcessorNotFoundException.class)
    public void canCatchException() throws Exception {
        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.when(IOUtils.toString(any(FileInputStream.class))).thenThrow(new IOException());

        processorExtractor = new LocalProcessorExtractor();
        processorExtractor.getProcessors(DUMMY_USER_NAME);
    }

    @Test
    public void canReturnEmptyBundleWhenNoBundlesFound() throws Exception {
        PowerMockito.mockStatic(PropertiesWrapper.class);
        PowerMockito.when(PropertiesWrapper.get(anyString())).thenReturn("dummyDir");

        processorExtractor = new LocalProcessorExtractor();
        List<WpsProcess> processors = processorExtractor.getProcessors(DUMMY_USER_NAME);

        assertThat(processors.size(), equalTo(0));
    }

    private ProcessorNameConverter getMatchingParser() {
        ProcessorNameConverter mockParser = mock(ProcessorNameConverter.class);
        when(mockParser.getBundleName()).thenReturn("urbantep-local");
        when(mockParser.getBundleVersion()).thenReturn("1.0");
        when(mockParser.getExecutableName()).thenReturn("Subset");
        return mockParser;
    }

    private ProcessorNameConverter getUnmatchingParser() {
        ProcessorNameConverter mockParser = mock(ProcessorNameConverter.class);
        when(mockParser.getBundleName()).thenReturn("unknown-bundle");
        when(mockParser.getBundleVersion()).thenReturn("0.0");
        when(mockParser.getExecutableName()).thenReturn("unknown-processor");
        return mockParser;
    }
}