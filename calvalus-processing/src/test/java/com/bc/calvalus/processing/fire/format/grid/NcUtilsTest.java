package com.bc.calvalus.processing.fire.format.grid;

import org.junit.Ignore;
import org.junit.Test;
import ucar.nc2.NetcdfFileWriter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;

public class NcUtilsTest {

    @Ignore
    @Test
    public void testCreateNcFile() throws Exception {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyymmdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault());
        String timeCoverageStart = dtf.format(LocalDate.of(2008, 6, 1).atTime(0, 0, 0));
        String timeCoverageEnd = dtf.format(LocalDate.of(2008, 6, 15).atTime(23, 59, 59));
        NetcdfFileWriter ncFile = NcUtils.createNcFile(".\\" + NcUtils.createFilename("2008", "06", true), "v4.1", timeCoverageStart, timeCoverageEnd, 15);
        ncFile.close();
    }

    @Test
    public void testCreateFilename() throws Exception {
        assertEquals("20080607-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", NcUtils.createFilename("2008", "06", true));
        assertEquals("20080622-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", NcUtils.createFilename("2008", "06", false));

        assertEquals("20101007-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", NcUtils.createFilename("2010", "10", true));
        assertEquals("20101022-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", NcUtils.createFilename("2010", "10", false));
    }

    @Test
    public void testCreateTimeString() throws Exception {
        String localTimeString = NcUtils.createTimeString(Instant.parse("2007-12-03T10:15:30.00Z"));
        assertEquals("2007-12-03 11:15:30", localTimeString);
    }

}