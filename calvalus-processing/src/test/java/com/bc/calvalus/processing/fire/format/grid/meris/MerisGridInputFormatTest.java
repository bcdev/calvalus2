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
        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default/2006/l3-2006-06-*-fire-nc/CCI-Fire-*2006-06-*-v04h01*.nc",
                MerisGridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2006/BA_PIX_MER_v04h01_200606_v4.0.tif"));

        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default/2005/l3-2005-06-*-fire-nc/CCI-Fire-*2005-06-*-v12h11*.nc",
                MerisGridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2005/BA_PIX_MER_v12h11_200506_v4.0.tif"));

        assertEquals("hdfs://calvalus/calvalus/projects/fire/sr-fr-default/2008/l3-2008-03-*-fire-nc/CCI-Fire-*2008-03-*-v12h11*.nc",
                MerisGridInputFormat.getSrInputPathPattern("hdfs://calvalus/calvalus/projects/fire/meris-ba/2008/BA_PIX_MER_v12h11_200803_v4.0.tif"));
    }
}