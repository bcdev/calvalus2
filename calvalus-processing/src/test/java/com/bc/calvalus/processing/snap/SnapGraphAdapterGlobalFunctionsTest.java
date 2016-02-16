package com.bc.calvalus.processing.snap;

import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class SnapGraphAdapterGlobalFunctionsTest {

    @Test
    public void testOcNnRdAuxdata_MERIS_TOMS() throws Exception {

        String merisProductName = "MER_RR__1PNACR20020523_103543_xyz";
        SnapGraphAdapter.GlobalsFunctions gf = new SnapGraphAdapter.GlobalsFunctions(Logger.getLogger("test"));

        String dateString = merisProductName.substring(14, 29);
        assertEquals("20020523_103543", dateString);
        Date productDate = gf.parseDate("yyyyMMdd_hhmmss", dateString);

        Calendar calendar = gf.getCalendar();
        calendar.setTime(productDate);

        Calendar timeOzone0 = gf.getCalendar();
        timeOzone0.clear();
        timeOzone0.set(1, calendar.get(1)); // Calendar.YEAR
        timeOzone0.set(2, calendar.get(2)); // Calendar.MONTH
        timeOzone0.set(5, calendar.get(5)); // Calendar.DAY_OF_MONTH

        Calendar timeOzone1 = gf.getCalendar();
        timeOzone1.setTime(timeOzone0.getTime());
        timeOzone1.add(5, 1); // Calendar.DAY_OF_MONTH

        String ozonePathFormat = "'AUX/'yyyy'/'DDD'/N'yyyyDDD'00_O3_TOMSOMI_24h.hdf'";
        assertEquals("AUX/2002/143/N200214300_O3_TOMSOMI_24h.hdf", gf.formatDate(ozonePathFormat, timeOzone0.getTime()));
        assertEquals("AUX/2002/144/N200214400_O3_TOMSOMI_24h.hdf", gf.formatDate(ozonePathFormat, timeOzone1.getTime()));
    }

    @Test
    public void testOcNnRdAuxdata_MERIS_NCEP() throws Exception {

        String merisProductName = "MER_RR__1PNACR20020523_103543_xyz";
        SnapGraphAdapter.GlobalsFunctions gf = new SnapGraphAdapter.GlobalsFunctions(Logger.getLogger("test"));

        String dateString = merisProductName.substring(14, 29);
        assertEquals("20020523_103543", dateString);
        Date productDate = gf.parseDate("yyyyMMdd_hhmmss", dateString);

        Calendar calendar = gf.getCalendar();
        calendar.setTime(productDate);

        Calendar timemeteo0 = gf.getCalendar();
        timemeteo0.clear();
        timemeteo0.set(1, calendar.get(1)); // Calendar.YEAR
        timemeteo0.set(2, calendar.get(2)); // Calendar.MONTH
        timemeteo0.set(5, calendar.get(5)); // Calendar.DAY_OF_MONTH
        timemeteo0.set(11, (6 * (calendar.get(11) / 6))); // Calendar.HOUR_OF_DAY

        Calendar timemeteo1 = gf.getCalendar();
        timemeteo1.setTime(timemeteo0.getTime());
        timemeteo1.add(11, 6); // Calendar.HOUR_OF_DAY

        String meteoPathFormat0 = "'AUX/'yyyy'/'DDD'/S'yyyyDDDhh'_NCEP.MET'";
        assertEquals("AUX/2002/143/S200214306_NCEP.MET", gf.formatDate(meteoPathFormat0, timemeteo0.getTime()));
        assertEquals("AUX/2002/143/S200214312_NCEP.MET", gf.formatDate(meteoPathFormat0, timemeteo1.getTime()));

        String meteoPathFormat1 = "'AUX/'yyyy'/'DDD'/N'yyyyDDDhh'_MET_NCEPN_6h.hdf'";
        assertEquals("AUX/2002/143/N200214306_MET_NCEPN_6h.hdf", gf.formatDate(meteoPathFormat1, timemeteo0.getTime()));
        assertEquals("AUX/2002/143/N200214312_MET_NCEPN_6h.hdf", gf.formatDate(meteoPathFormat1, timemeteo1.getTime()));
    }

    @Test
    public void testOcNnRdAuxdata_MODIS_TOMS() throws Exception {

        String modisProductName = "A2003001000000.L1B_LAC";
        SnapGraphAdapter.GlobalsFunctions gf = new SnapGraphAdapter.GlobalsFunctions(Logger.getLogger("test"));

        String dateString = modisProductName.substring(1, 14);
        assertEquals("2003001000000", dateString);
        Date productDate = gf.parseDate("yyyyDDDhhmmss", dateString);

        Calendar calendar = gf.getCalendar();
        calendar.setTime(productDate);

        Calendar timeOzone0 = gf.getCalendar();
        timeOzone0.clear();
        timeOzone0.set(1, calendar.get(1)); // Calendar.YEAR
        timeOzone0.set(2, calendar.get(2)); // Calendar.MONTH
        timeOzone0.set(5, calendar.get(5)); // Calendar.DAY_OF_MONTH

        Calendar timeOzone1 = gf.getCalendar();
        timeOzone1.setTime(timeOzone0.getTime());
        timeOzone1.add(5, 1); // Calendar.DAY_OF_MONTH

        String ozonePathFormat = "'AUX/'yyyy'/'DDD'/N'yyyyDDD'00_O3_TOMSOMI_24h.hdf'";
        assertEquals("AUX/2003/001/N200300100_O3_TOMSOMI_24h.hdf", gf.formatDate(ozonePathFormat, timeOzone0.getTime()));
        assertEquals("AUX/2003/002/N200300200_O3_TOMSOMI_24h.hdf", gf.formatDate(ozonePathFormat, timeOzone1.getTime()));
    }

}
