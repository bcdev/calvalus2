package com.bc.calvalus.processing.fire.format.pixel.meris;

import com.bc.calvalus.processing.fire.format.MerisStrategy;
import com.bc.calvalus.processing.fire.format.PixelProductArea;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

public class MerisPixelMergeMapperTest {

    @Ignore
    @Test
    public void testCreateMetadata() throws Exception {
        String version = "v04.1";
        for (String year : new String[]{"2005", "2006", "2007", "2008", "2009", "2010", "2011"}) {
            for (String month : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}) {
                for (PixelProductArea area : new MerisStrategy().getAllAreas()) {
                    String baseFilename = MerisPixelMergeMapper.createBaseFilename(year, month, version, area);
                    File file = new File(baseFilename + ".xml");
                    try (FileWriter fileWriter = new FileWriter(file)) {
                        String metadata = MerisPixelMergeMapper.createMetadata(year, month, version, area);
                        fileWriter.write(metadata);
                    }
                }
            }
        }
    }
}