package com.bc.calvalus.processing.fire;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 */
public class FireGridInputFormatTest {

    @Test
    public void getLcInputPath() throws Exception {
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2005-v04h01.nc",
                FireGridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2006/BA_PIX_MER_v04h01_200606_v4.0.tif"));
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2005-v12h11.nc",
                FireGridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2005/BA_PIX_MER_v12h11_200506_v4.0.tif"));
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2010-v12h11.nc",
                FireGridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2008/BA_PIX_MER_v12h11_200806_v4.0.tif"));
    }

}