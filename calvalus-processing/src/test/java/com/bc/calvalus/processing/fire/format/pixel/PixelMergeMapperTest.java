package com.bc.calvalus.processing.fire.format.pixel;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

public class PixelMergeMapperTest {

    @Ignore
    @Test
    public void testCreateMetadata() throws Exception {
        String version = "v04.1";
        for (String year : new String[]{"2005", "2006", "2007", "2008", "2009", "2010", "2011"}) {
            for (String month : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}) {
                for (PixelProductArea area : PixelProductArea.values()) {
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
}