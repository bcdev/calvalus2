package com.bc.calvalus.processing.fire;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S2BaInputFormatTest {

    @Test
    public void testGetPeriodInputPathPattern() throws Exception {
        assertEquals("hdfs://calvalus/calvalus/projects/fire/s2-pre/.*/.*/.*/.*T30PVR.tif$", S2BaInputFormat.getPeriodInputPathPattern(
                "hdfs://calvalus/calvalus/projects/fire/s2-pre/2016/01/16/S2A_USER_MTD_SAFL2A_PDMC_20160116T175154_R108_V20160116T105012_20160116T105012_T30PVR.tif"
        ));

    }

}