package com.bc.calvalus.processing.fire;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class FireBAInputFormatTest {

    @Test
    public void testStringMatchesTile() throws Exception {

        assertTrue(FireBAInputFormat.stringMatchesTile(19, 0, "CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-04-02-v00h19.nc"));
        assertTrue(FireBAInputFormat.stringMatchesTile(19, 18, "CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-04-02-v18h19.nc"));
        assertTrue(FireBAInputFormat.stringMatchesTile(3, 2, "CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-04-02-v02h03.nc"));

        String path = "hdfs://calvalus/calvalus/projects/fire/meris-ba/2008/v1/auxdata-2008-v00h26.tar.gz";
        int startIndex = path.toString().lastIndexOf("/") + 1;

        System.out.println(path.substring(startIndex));

    }

}