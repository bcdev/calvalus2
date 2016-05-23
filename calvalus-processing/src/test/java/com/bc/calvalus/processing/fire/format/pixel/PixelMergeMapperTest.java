package com.bc.calvalus.processing.fire.format.pixel;

import org.junit.Test;

public class PixelMergeMapperTest {

    @Test
    public void testCreateMetadata() throws Exception {
        PixelMergeMapper.createMetadata("2002", "05", PixelProductArea.EUROPE);
    }

}