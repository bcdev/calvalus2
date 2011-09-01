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

        writer.processHeaderRecord(createDataHeader("CHL"));
        writer.processDataRecord(0, createDataRecord1(0.7));
        writer.processDataRecord(1, createDataRecord2(0.3));
        writer.processDataRecord(2, createDataRecord3(1.4));
        writer.processDataRecord(3, createDataRecord4(Double.NaN));
        writer.processDataRecord(4, createDataRecord4(null));
        writer.finalizeRecordProcessing(4);

        assertEquals("" +
                             "ID\tSITE\tLAT\tLON\tTIME\tCHL\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.7\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.3\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.4\n" +
                             "\t\tNaN\t13.9\t\tNaN\n" +
                             "\t\tNaN\t13.9\t\t\n" +
                             "",
                     recordsAllWriter.toString());
        assertEquals("" +
                             "ID\tSITE\tLAT\tLON\tTIME\tCHL\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.7\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.3\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.4\n" +
                             "\t\tNaN\t13.9\t\tNaN\n" +
                             "\t\tNaN\t13.9\t\t\n" +
                             "",
                     recordsAggWriter.toString());
    }

    @Test
    public void testWithAggregatedNumbersWithoutDataArrays() throws Exception {

        StringWriter recordsAllWriter = new StringWriter();
        StringWriter recordsAggWriter = new StringWriter();
        CsvRecordWriter writer = new CsvRecordWriter(recordsAllWriter, recordsAggWriter);

        writer.processHeaderRecord(createDataHeader("*CHL"));
        writer.processDataRecord(0, createDataRecord1(new AggregatedNumber(3, 3, 0, 0.0, 1.0, 0.7, 0.01)));
        writer.processDataRecord(1, createDataRecord2(new AggregatedNumber(2, 3, 1, 0.0, 1.0, 0.3, 0.02)));
        writer.processDataRecord(2, createDataRecord3(new AggregatedNumber(3, 3, 0, 0.0, 1.0, 1.4, 0.03)));
        writer.processDataRecord(3, createDataRecord4(new AggregatedNumber(0, 0, 0, 0.0, 0.0, Double.NaN, 0.0)));
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

    @Test
    public void testWithAggregatedNumbersWithDataArrays() throws Exception {

        StringWriter recordsAllWriter = new StringWriter();
        StringWriter recordsAggWriter = new StringWriter();
        CsvRecordWriter writer = new CsvRecordWriter(recordsAllWriter, recordsAggWriter);

        writer.processHeaderRecord(createDataHeader("*CHL"));
        writer.processDataRecord(0, createDataRecord1(new AggregatedNumber(3, 3, 0, 0.0, 1.0, 0.7, 0.01,
                                                                           new float[] {0.69F, 0.7F, 0.71F})));
        writer.processDataRecord(1, createDataRecord2(new AggregatedNumber(2, 3, 1, 0.0, 1.0, 0.3, 0.01,
                                                                           new float[] {0.29F, 0.3F, 0.31F})));
        writer.processDataRecord(2, createDataRecord3(new AggregatedNumber(3, 3, 0, 0.0, 1.0, 1.4, 0.01,
                                                                           new float[] {1.39F, 1.4F, 1.41F})));
        writer.processDataRecord(3, createDataRecord4(new AggregatedNumber(0, 0, 0, 0.0, 0.0, Double.NaN, 0.0,
                                                                           new float[] {Float.NaN, Float.NaN, Float.NaN})));
        writer.finalizeRecordProcessing(4);

        assertEquals("" +
                             "ID\tSITE\tLAT\tLON\tTIME\tCHL\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.69\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.7\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.71\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.29\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.3\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.31\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.39\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.4\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.41\n" +
                             "\t\tNaN\t13.9\t\tNaN\n" +
                             "\t\tNaN\t13.9\t\tNaN\n" +
                             "\t\tNaN\t13.9\t\tNaN\n" +
                             "",
                     recordsAllWriter.toString());
        assertEquals("" +
                             "ID\tSITE\tLAT\tLON\tTIME\tCHL_mean\tCHL_sigma\tCHL_n\n" +
                             "536\tA\t53.1\t13.9\t2011-09-01 07:07:53\t0.7\t0.01\t3\n" +
                             "538\tB\t53.1\t13.9\t2011-09-01 07:07:54\t0.3\t0.01\t2\n" +
                             "539\tC\t53.1\t13.9\t2011-09-01 07:07:55\t1.4\t0.01\t3\n" +
                             "\t\tNaN\t13.9\t\tNaN\t0.0\t0\n" +
                             "",
                     recordsAggWriter.toString());
    }

    private static Object[] createDataHeader(String chl) {
        return new Object[]{"ID", "SITE", "LAT", "LON", "TIME", chl};
    }

    private static Object[] createDataRecord1(Object chl) {
        return new Object[]{536, "A", 53.1F, 13.9F, new Date(1314860873000L), chl};
    }

    private static Object[] createDataRecord2(Object number) {
        return new Object[]{538, "B", 53.1F, 13.9F, new Date(1314860874000L), number};
    }

    private static Object[] createDataRecord3(Object chl) {
        return new Object[]{539, "C", 53.1F, 13.9F, new Date(1314860875000L), chl};
    }

    private static Object[] createDataRecord4(Object chl) {
        return new Object[]{null, "", Float.NaN, 13.9F, null, chl};
    }
}
