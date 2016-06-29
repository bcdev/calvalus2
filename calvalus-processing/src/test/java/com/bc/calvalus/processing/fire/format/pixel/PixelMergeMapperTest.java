package com.bc.calvalus.processing.fire.format.pixel;

import org.junit.Test;

public class PixelMergeMapperTest {

    @Test
    public void testCreateMetadata() throws Exception {
        String metadata = PixelMergeMapper.createMetadata("2002", "05", "04.1", PixelProductArea.EUROPE);
        System.out.println(metadata);
    }

}