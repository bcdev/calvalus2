package com.bc.calvalus.reporting.ws;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.bc.calvalus.reporting.tools.UsageStatisticCsvConverter;
import org.esa.snap.core.util.io.CsvReader;
import org.junit.*;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

/**
 * @author hans
 */
public class ReportGeneratorTest {

    private ReportGenerator reportGenerator;

    private UsageStatisticCsvConverter converter;

    @Before
    public void setUp() throws Exception {
        Reader sampleCsvReader = new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("sampleUsageCsv.csv"));
        CsvReader csvReader = new CsvReader(sampleCsvReader, new char[]{','});
        converter = new UsageStatisticCsvConverter(csvReader.readStringRecords());
    }

    @Test
    public void canGenerateTextSingleJob() throws Exception {
        UsageStatistic usageStatistic = converter.extractSingleStatistic("job_1481485063251_7037");
        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateTextSingleJob(usageStatistic), equalTo("Usage statistic for job 'job_1481485063251_7037'\n" +
                                                                                  "\n" +
                                                                                  "Project : default\n" +
                                                                                  "Start time : 29.12.2016 11:33:18\n" +
                                                                                  "Finish time : 29.12.2016 13:50:23\n" +
                                                                                  "Total time : 02:17:04\nStatus :  SUCCEEDED\n" +
                                                                                  "Total file writing (MB) : 695\n" +
                                                                                  "Total file reading (MB) : 789,736\n" +
                                                                                  "Total CPU time spent : 20:15:07\n" +
                                                                                  "Total Memory used (MB s) :  156,226,661\n" +
                                                                                  "Total vCores used (vCore s) :  152,565\n"));
    }

    @Ignore // to avoid creating pdf in every maven install
    @Test
    public void canGeneratePdfSingleJob() throws Exception {
        UsageStatistic usageStatistic = converter.extractSingleStatistic("job_1481485063251_7037");
        reportGenerator = new ReportGenerator();
        String pdfPath = reportGenerator.generatePdfSingleJob(usageStatistic);

        assertThat(pdfPath, containsString("job_1481485063251_7037.pdf"));
    }

    @Test
    public void canGenerateTextMonthly() throws Exception {
        List<UsageStatistic> usageStatistics = converter.extractAllStatistics();

        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateTextMonthly(usageStatistics), equalTo("Usage statistic for user $USER in $MONTH $YEAR\n" +
                                                                                 "\n" +
                                                                                 "Jobs processed : 3\nTotal file writing (MB) : 651,102\n" +
                                                                                 "Total file reading (MB) : 1,219,433\n" +
                                                                                 "Total CPU time spent : 27:04:53\n" +
                                                                                 "Total Memory used (MB s) :  599,478,162\n" +
                                                                                 "Total vCores used (vCore s) :  239,137\n" +
                                                                                 "\n" +
                                                                                 "\n" +
                                                                                 "Price breakdown\n" +
                                                                                 "\n" +
                                                                                 "CPU usage price = (Total vCores used) x € 0.0013 = € 0.09\n" +
                                                                                 "Memory usage price = (Total Memory used) x € 0.00022 = € 0.04\n" +
                                                                                 "Disk space usage price = (Total file writing GB + Total file reading GB) x € 0.011 = € 20.45\n" +
                                                                                 "\n" +
                                                                                 "Total = € 20.58\n"));
    }

    @Ignore // to avoid creating pdf in every maven install
    @Test
    public void canGeneratePdfMonthly() throws Exception {
        List<UsageStatistic> usageStatistics = converter.extractAllStatistics();

        reportGenerator = new ReportGenerator();
        String pdfPath = reportGenerator.generatePdfMonthly(usageStatistics);

        assertThat(pdfPath, containsString("monthly.pdf"));
    }
}