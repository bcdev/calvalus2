package com.bc.calvalus.processing.fire.format.grid.meris;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 */
public class MerisGridInputFormatTest {

    @Test
    public void getLcInputPath() throws Exception {
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2000-v04h01.nc",
                MerisGridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2006/BA_PIX_MER_v04h01_200606_v4.0.tif"));
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2000-v12h11.nc",
                MerisGridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2005/BA_PIX_MER_v12h11_200506_v4.0.tif"));
        assertEquals("hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2005-v12h11.nc",
                MerisGridInputFormat.getLcInputPath("hdfs://calvalus/calvalus/projects/fire/meris-ba/2008/BA_PIX_MER_v12h11_200806_v4.0.tif"));
    }

    @Test
    public void getSrInputPath() throws Exception {
        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2006/v04h01/2006/2006-06-*/CCI-Fire-*.nc",
                MerisGridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2006/BA_PIX_MER_v04h01_200606_v4.0.tif"));

        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2005/v12h11/2005/2005-06-*/CCI-Fire-*.nc",
                MerisGridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2005/BA_PIX_MER_v12h11_200506_v4.0.tif"));

        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/v12h11/2008/2008-03-*/CCI-Fire-*.nc",
                MerisGridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2008/BA_PIX_MER_v12h11_200803_v4.0.tif"));
    }
}