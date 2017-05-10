package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class SeasonalCompositingMapperTest {

    private static final SimpleDateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");

    @Test
    public void testNextWeek() throws Exception {
        Date d = DATE_FORMAT.parse("2012-01-01");
        Calendar start = DateUtils.createCalendar();
        start.setTime(DATE_FORMAT.parse("2012-01-01"));
        Calendar stop = DateUtils.createCalendar();
        stop.setTime(DATE_FORMAT.parse("2012-12-31"));
        GregorianCalendar c = DateUtils.createCalendar();
        c.setTime(d);
        assertEquals(0, c.get(Calendar.MONTH)) ;
        assertEquals(1, c.get(Calendar.DAY_OF_MONTH)) ;
        for (int i = 0; i < 52; i++) {
            final int l = SeasonalCompositingMapper.lengthOfWeek(c);
            if ((c.get(Calendar.MONTH) == Calendar.FEBRUARY && c.get(Calendar.DAY_OF_MONTH) == 26) ||
                (c.get(Calendar.MONTH) == Calendar.DECEMBER && c.get(Calendar.DAY_OF_MONTH) == 24)) {
                assertEquals(8, l);
            } else {
                assertEquals(7, l);
            }
            d = SeasonalCompositingMapper.nextWeek(d, start, stop, 7);
            c.setTime(d);
        }
        assertEquals("2013-01-01", DATE_FORMAT.format(d));
    }

    @Test
    public void testBandMapping() {
        String sensorAndResolution = "MERIS-300m";
        Configuration conf = new Configuration();
        conf.set("calvalus.seasonal.bands", "sr_2_mean,sr_3_mean,sr_6_mean,sr_7_mean,sr_8_mean");

        String[] sensorBands = SeasonalCompositingMapper.sensorBandsOf(sensorAndResolution);
        String[] targetBands = SeasonalCompositingMapper.targetBandsOf(conf, sensorBands);
        int[] targetBandIndex = new int[17];  // desired band for the seasonal composite, as index to sensorBands
        int[] sourceBandIndex = new int[20];  // corresponding required band of the L3 product, as index to product
        int numTargetBands = 3;
        int numSourceBands = 6;
        for (int j = 0; j < 3; ++j) {
            targetBandIndex[j] = j;
        }
        for (int i = 0; i < 6; ++i) {
            sourceBandIndex[i] = i;
        }
        for (int j = 3; j < sensorBands.length && numTargetBands < targetBands.length; ++j) {
            if (sensorBands[j].equals(targetBands[numTargetBands])) {  // sequence is important
                targetBandIndex[numTargetBands++] = j;
                sourceBandIndex[numSourceBands++] = SeasonalCompositingMapper.sourceBandIndexOf(sensorAndResolution, j);
            }
        }

        assertEquals("numTargetBands", 5 + 3, numTargetBands);
        assertEquals("numSourceBands", 5 + 6, numSourceBands);
        assertEquals("targetBandIndex", 9, targetBandIndex[6]);
        assertEquals("sourceBandIndex", 18, sourceBandIndex[9]);
        assertEquals("targetBandIndex", 10, targetBandIndex[7]);
        assertEquals("sourceBandIndex", 20, sourceBandIndex[10]);
    }
}
