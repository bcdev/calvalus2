package com.bc.calvalus.reporting;

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
public class ReportGeneratorTest {

    private ReportGenerator reportGenerator;

    private UsageStatisticConverter converter;

    @Before
    public void setUp() throws Exception {
        Reader sampleCsvReader = new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("sampleUsageCsv.csv"));
        CsvReader csvReader = new CsvReader(sampleCsvReader, new char[]{','});
        converter = new UsageStatisticConverter(csvReader.readStringRecords());
    }

    @Test
    public void canGenerateTextSingleJob() throws Exception {
        UsageStatistic usageStatistic = converter.extractSingleStatistic("job_1481485063251_7037");
        reportGenerator = new ReportGenerator();

        assertThat(reportGenerator.generateTextSingleJob(usageStatistic), equalTo("Usage statistic for job 'job_1481485063251_7037'\n" +
                                                                                  "\n" +
                                                                                  "Project : default\n" +
                                                                                  "Start time : 29-Dec-2016 11:33:18\n" +
                                                                                  "Finish time : 29-Dec-2016 13:50:23\n" +
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
                                                                                 "Total vCores used (vCore s) :  239,137\n"));
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