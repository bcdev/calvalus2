package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.processing.fire.format.S2Strategy;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S2PixelInputFormatTest {

    private S2Strategy s2Strategy;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        conf.set("sensor", "MERIS");
        s2Strategy = new S2Strategy(conf);
    }

    @Test
    public void testGetInputPathPattern_1() throws Exception {
        String inputPathPattern = S2PixelInputFormat.getInputPathPattern("2015", "03", s2Strategy.getArea("AREA_1"));

        String tiles = "26NQG|26NQH|26NQJ|26NQK|26NRG|26NRH|26NRJ|26NRK|27NTB|27NTC|27NTD|27NTE|27NUB|27NUC|27NUD|27NUE";
        assertEquals("hdfs://calvalus/calvalus/projects/fire/s2-ba/BA-." + tiles + "-201503.*nc", inputPathPattern);
    }

    @Test
    public void testGetInputPathPattern_2() throws Exception {
        String inputPathPattern = S2PixelInputFormat.getInputPathPattern("2016", "12", s2Strategy.getArea("AREA_15"));

        String tiles = "38NMG|38NMH|38NMJ|38NMK|38NNG|38NNH|38NNJ|38NNK|38NPG|38NPH|38NPJ|38NPK|38NQG|38NQH|38NQJ|38NQK|38NRG|38NRH|38NRJ|38NRK";
        assertEquals("hdfs://calvalus/calvalus/projects/fire/s2-ba/BA-." + tiles + "-201612.*nc", inputPathPattern);
    }

    @Test
    public void getGranule() throws Exception {
        String s2Granule = S2PixelInputFormat.getGranule("hdfs://calvalus/calvalus/projects/fire/s2-ba/BA-T32NPN-20160210T095818.nc");
        assertEquals("32NPN", s2Granule);
    }
}