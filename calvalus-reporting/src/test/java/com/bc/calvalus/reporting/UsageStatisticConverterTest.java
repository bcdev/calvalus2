package com.bc.calvalus.reporting;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.esa.snap.core.util.io.CsvReader;
import org.junit.*;

import java.io.InputStreamReader;
import java.io.Reader;

/**
 * @author hans
 */
public class UsageStatisticConverterTest {

    @Test
    public void canExtractUsageStatistics() throws Exception {
        Reader sampleCsvReader = new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("sampleUsageCsv.csv"));
        CsvReader csvReader = new CsvReader(sampleCsvReader, new char[]{','});
        UsageStatisticConverter converter = new UsageStatisticConverter(csvReader.readStringRecords());

        assertThat(converter.getUsageStatistics().size(), equalTo(3));
        assertThat(converter.getUsageStatistics().get(0).getJobId(), equalTo("job_1481485063251_7044"));
        assertThat(converter.getUsageStatistics().get(0).getQueueName(), equalTo("other"));
        assertThat(converter.getUsageStatistics().get(0).getStartTime(), equalTo(1483008371365L));
        assertThat(converter.getUsageStatistics().get(0).getFinishTime(), equalTo(1483016032298L));
        assertThat(converter.getUsageStatistics().get(0).getStatus(), equalTo("SUCCEEDED"));
        assertThat(converter.getUsageStatistics().get(0).getFileBytesRead(), equalTo(0L));
        assertThat(converter.getUsageStatistics().get(0).getFileBytesWritten(), equalTo(66806953L));
        assertThat(converter.getUsageStatistics().get(0).getHdfsBytesRead(), equalTo(244480290008L));
        assertThat(converter.getUsageStatistics().get(0).getHdfsBytesWritten(), equalTo(370862346404L));
        assertThat(converter.getUsageStatistics().get(0).getMbMillisMaps(), equalTo(248551562240L));
        assertThat(converter.getUsageStatistics().get(0).getMbMillisReduces(), equalTo(0L));
        assertThat(converter.getUsageStatistics().get(0).getvCoresMillisMaps(), equalTo(48545227L));
        assertThat(converter.getUsageStatistics().get(0).getvCoresMillisReduces(), equalTo(0L));
        assertThat(converter.getUsageStatistics().get(0).getCpuMilliseconds(), equalTo(13273330L));
    }
}