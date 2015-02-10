package com.bc.calvalus.processing.l3.seasonal;

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

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    @Test
    public void testNextWeek() throws Exception {
        Date d = DATE_FORMAT.parse("2012-01-01");
        GregorianCalendar c = new GregorianCalendar();
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
            d = SeasonalCompositingMapper.nextWeek(d);
            c.setTime(d);
        }
        assertEquals("2013-01-01", DATE_FORMAT.format(d));
    }
}
