package com.bc.calvalus.processing.fire.format.grid.meris;

import org.junit.Ignore;
import org.junit.Test;
import ucar.nc2.NetcdfFileWriter;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class MerisNcFileFactoryTest {

    @Ignore
    @Test
    public void acceptanceTestCreateNcFile() throws Exception {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyymmdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault());
        String timeCoverageStart = dtf.format(LocalDate.of(2008, 6, 1).atTime(0, 0, 0));
        String timeCoverageEnd = dtf.format(LocalDate.of(2008, 6, 15).atTime(23, 59, 59));
        NetcdfFileWriter ncFile = new MerisNcFileFactory().createNcFile(".\\" + String.format("%s%s%s-ESACCI-L4_FIRE-BA-MERIS-f%s.nc", "2008", "06", true ? "07" : "22", "04.1"), "v4.1", timeCoverageStart, timeCoverageEnd, 15);
        ncFile.close();
    }

}