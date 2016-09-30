package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.processing.fire.format.MerisStrategy;
import com.bc.calvalus.processing.fire.format.PixelProductArea;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

import static org.junit.Assert.assertEquals;

public class PixelMergeMapperTest {

    @Ignore
    @Test
    public void testCreateMetadata() throws Exception {
        String version = "v04.1";
        for (String year : new String[]{"2005", "2006", "2007", "2008", "2009", "2010", "2011"}) {
            for (String month : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}) {
                for (PixelProductArea area : new MerisStrategy().getAllAreas()) {
                    String baseFilename = PixelMergeMapper.createBaseFilename(year, month, version, area);
                    File file = new File(baseFilename + ".xml");
                    try (FileWriter fileWriter = new FileWriter(file)) {
                        String metadata = PixelMergeMapper.createMetadata(year, month, version, area);
                        fileWriter.write(metadata);
                    }
                }
            }
        }
    }

    @Test
    public void testCreateBaseFilename() throws Exception {
        assertEquals("20030501-ESACCI-L3S_FIRE-BA-MERIS-AREA_3-fv04.1", PixelMergeMapper.createBaseFilename("2003", "05", "v04.1", getArea("EUROPE")));
        assertEquals("20101001-ESACCI-L3S_FIRE-BA-MERIS-AREA_4-fv04.1", PixelMergeMapper.createBaseFilename("2010", "10", "v04.1", getArea("ASIA")));
    }

    private static PixelProductArea getArea(String id) {
        return new MerisStrategy().getArea(id);
    }

}