package com.bc.calvalus.reporting.tools;

import com.bc.calvalus.reporting.io.JSONExtractor;

/**
 * @author hans
 */
public class UsageStatisticJsonConverterTest {

    private JSONExtractor mockJsonExtractor;

    /*@Before
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

        jsonConverter.extractAllStatistics("2017-02-10");
//        verify(mockJsonExtractor, times(1)).getAllStatistics(PropertiesWrapper.get("reporting.folder.path"));
    }

    @Test(expected = ExtractionException.class)
    public void canCatchIOExceptionWhenExtractAll() throws Exception {
//        when(mockJsonExtractor.getAllStatistics(PropertiesWrapper.get("reporting.folder.path"))).thenThrow(new IOException("Unable to extract statistics"));
//        UsageStatisticJsonConverter jsonConverter = new UsageStatisticJsonConverter(mockJsonExtractor);
//
//        jsonConverter.extractAllStatistics();
    }*/

}