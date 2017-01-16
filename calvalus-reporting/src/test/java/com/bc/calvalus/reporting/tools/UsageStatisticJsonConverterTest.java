package com.bc.calvalus.reporting.tools;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.reporting.exceptions.ExtractionException;
import com.bc.calvalus.reporting.io.JSONExtractor;
import com.bc.calvalus.reporting.ws.NullUsageStatistic;
import org.junit.*;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

/**
 * @author hans
 */
public class UsageStatisticJsonConverterTest {

    private JSONExtractor mockJsonExtractor;

    @Before
    public void setUp() throws Exception {
        mockJsonExtractor = mock(JSONExtractor.class);
        when(mockJsonExtractor.getSingleStatistic(anyString())).thenReturn(new NullUsageStatistic());
    }

    @Test
    public void canExtractSingleStatistic() throws Exception {
        UsageStatisticJsonConverter jsonConverter = new UsageStatisticJsonConverter(mockJsonExtractor);

        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);

        jsonConverter.extractSingleStatistic("job_xxxxx");
        verify(mockJsonExtractor, times(1)).getSingleStatistic(jobIdCaptor.capture());

        assertThat(jobIdCaptor.getValue(), equalTo("job_xxxxx"));
    }

    @Test
    public void canExtractAllStatistics() throws Exception {
        UsageStatisticJsonConverter jsonConverter = new UsageStatisticJsonConverter(mockJsonExtractor);

        jsonConverter.extractAllStatistics();
        verify(mockJsonExtractor, times(1)).getAllStatistics();
    }

    @Test(expected = ExtractionException.class)
    public void canCatchIOExceptionWhenExtractAll() throws Exception {
        when(mockJsonExtractor.getAllStatistics()).thenThrow(new IOException("Unable to extract statistics"));
        UsageStatisticJsonConverter jsonConverter = new UsageStatisticJsonConverter(mockJsonExtractor);

        jsonConverter.extractAllStatistics();
    }

}