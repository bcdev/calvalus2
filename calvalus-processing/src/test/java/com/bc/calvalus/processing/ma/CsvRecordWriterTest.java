package com.bc.calvalus.processing.ma;

import org.junit.Test;

import java.io.StringWriter;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class CsvRecordWriterTest {
    @Test
    public void testWithScalarsOnly() throws Exception {

        StringWriter recordsAllWriter = new StringWriter();
        StringWriter recordsAggWriter = new StringWriter();
        CsvRecordWriter writer = new CsvRecordWriter(recordsAllWriter, recordsAggWriter);

        writer.processHeaderRecord(new Object[]{"ID", "SITE", "LAT", "LON", "TIME", "CHL"});
        writer.processDataRecord(0, new Object[]{536, "A", 53.1F, 13.9F, new Date(1314860873000L), 0.7});
        writer.processDataRecord(1, new Object[]{538, "B", 53.1F, 13.9F, new Date(1314860874000L), 0.3});
        writer.processDataRecord(2, new Object[]{539, "C", 53.1F, 13.9F, new Date(1314860875000L), 1.4});
        writer.processDataRecord(3, new Object[]{null, "", Float.NaN, 13.9F, null, Double.NaN});
        writer.finalizeRecordProcessing(4);

        assertEquals("" +
                             "ID\tSITE\tLAT\tLON\tTIME\tCHL\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.7\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.3\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.4\n" +
                             "\t\tNaN\t13.9\t\tNaN\n" +
                             "",
                     recordsAllWriter.toString());
        assertEquals("" +
                             "ID\tSITE\tLAT\tLON\tTIME\tCHL\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.7\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.3\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.4\n" +
                             "\t\tNaN\t13.9\t\tNaN\n" +
                             "",
                     recordsAggWriter.toString());
    }

    @Test
    public void testWithAggregatedNumbersWithDataArrays() throws Exception {

        StringWriter recordsAllWriter = new StringWriter();
        StringWriter recordsAggWriter = new StringWriter();
        CsvRecordWriter writer = new CsvRecordWriter(recordsAllWriter, recordsAggWriter);

        writer.processHeaderRecord(new Object[]{"ID", "SITE", "LAT", "LON", "TIME", "*CHL"});
        writer.processDataRecord(0, new Object[]{536, "A", 53.1F, 13.9F, new Date(1314860873000L), new AggregatedNumber(3, 3, 0, 0.0, 1.0, 0.7, 0.01)});
        writer.processDataRecord(1, new Object[]{538, "B", 53.1F, 13.9F, new Date(1314860874000L), new AggregatedNumber(2, 3, 1, 0.0, 1.0, 0.3, 0.02)});
        writer.processDataRecord(2, new Object[]{539, "C", 53.1F, 13.9F, new Date(1314860875000L), new AggregatedNumber(3, 3, 0, 0.0, 1.0, 1.4, 0.03)});
        writer.processDataRecord(3, new Object[]{null, "", Float.NaN, 13.9F, null, new AggregatedNumber(0, 0, 0, 0.0, 0.0, Double.NaN, 0.0)});
        writer.finalizeRecordProcessing(4);

        assertEquals("" +
                             "ID\tSITE\tLAT\tLON\tTIME\tCHL\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.7\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.3\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.4\n" +
                             "\t\tNaN\t13.9\t\tNaN\n" +
                             "",
                     recordsAllWriter.toString());
        assertEquals("" +
                             "ID\tSITE\tLAT\tLON\tTIME\tCHL_mean\tCHL_sigma\tCHL_n\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.7\t0.01\t3\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.3\t0.02\t2\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.4\t0.03\t3\n" +
                             "\t\tNaN\t13.9\t\tNaN\t0.0\t0\n" +
                             "",
                     recordsAggWriter.toString());
    }
}
