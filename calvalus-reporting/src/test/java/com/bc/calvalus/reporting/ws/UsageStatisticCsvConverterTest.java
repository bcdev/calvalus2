package com.bc.calvalus.reporting.ws;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.esa.snap.core.util.io.CsvReader;
import org.junit.*;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

/**
 * @author hans
 */
public class UsageStatisticCsvConverterTest {

    private UsageStatisticCsvConverter converter;

    @Test
    public void canExtractUsageStatistics() throws Exception {
        Reader sampleCsvReader = new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("sampleUsageCsv.csv"));
        CsvReader csvReader = new CsvReader(sampleCsvReader, new char[]{','});
        converter = new UsageStatisticCsvConverter(csvReader.readStringRecords());
        List<UsageStatistic> usageStatistics = converter.extractAllStatistics();

        assertThat(usageStatistics.size(), equalTo(3));
        assertThat(usageStatistics.get(0).getJobId(), equalTo("job_1481485063251_7044"));
        assertThat(usageStatistics.get(0).getQueueName(), equalTo("other"));
        assertThat(usageStatistics.get(0).getStartTime(), equalTo(1483008371365L));
        assertThat(usageStatistics.get(0).getFinishTime(), equalTo(1483016032298L));
        assertThat(usageStatistics.get(0).getStatus(), equalTo("SUCCEEDED"));
        assertThat(usageStatistics.get(0).getFileBytesRead(), equalTo(0L));
        assertThat(usageStatistics.get(0).getFileBytesWritten(), equalTo(66806953L));
        assertThat(usageStatistics.get(0).getHdfsBytesRead(), equalTo(244480290008L));
        assertThat(usageStatistics.get(0).getHdfsBytesWritten(), equalTo(370862346404L));
        assertThat(usageStatistics.get(0).getMbMillisMaps(), equalTo(248551562240L));
        assertThat(usageStatistics.get(0).getMbMillisReduces(), equalTo(0L));
        assertThat(usageStatistics.get(0).getvCoresMillisMaps(), equalTo(48545227L));
        assertThat(usageStatistics.get(0).getvCoresMillisReduces(), equalTo(0L));
        assertThat(usageStatistics.get(0).getCpuMilliseconds(), equalTo(13273330L));
    }

    @Test
    public void canExtractSingleUsageStatistic() throws Exception {
        Reader sampleCsvReader = new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("sampleUsageCsv.csv"));
        CsvReader csvReader = new CsvReader(sampleCsvReader, new char[]{','});
        converter = new UsageStatisticCsvConverter(csvReader.readStringRecords());
        UsageStatistic usageStatistic = converter.extractSingleStatistic("job_1481485063251_7024");

        assertThat(usageStatistic, not(instanceOf(NullUsageStatistic.class)));
        assertThat(usageStatistic.getJobId(), equalTo("job_1481485063251_7024"));
    }

    @Test
    public void canReturnNullUsageStatistic() throws Exception {
        Reader sampleCsvReader = new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("sampleUsageCsv.csv"));
        CsvReader csvReader = new CsvReader(sampleCsvReader, new char[]{','});
        converter = new UsageStatisticCsvConverter(csvReader.readStringRecords());
        UsageStatistic usageStatistic = converter.extractSingleStatistic("job_xxxxx_invalid");

        assertThat(usageStatistic, instanceOf(NullUsageStatistic.class));
    }
}