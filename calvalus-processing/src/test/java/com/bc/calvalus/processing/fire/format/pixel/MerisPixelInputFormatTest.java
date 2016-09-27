package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.processing.fire.format.MerisStrategy;
import org.junit.Assert;
import org.junit.Test;

public class MerisPixelInputFormatTest {

    @Test
    public void getInputPathPatterns() throws Exception {

        String expected = "hdfs://calvalus/calvalus/projects/fire/meris-ba/2002/.*" +
                "(v00h15|v00h16|v00h17|v00h18|v00h19|v00h20|v00h21|v00h22|v00h23|" +
                "v01h15|v01h16|v01h17|v01h18|v01h19|v01h20|v01h21|v01h22|v01h23|" +
                "v02h15|v02h16|v02h17|v02h18|v02h19|v02h20|v02h21|v02h22|v02h23|" +
                "v03h15|v03h16|v03h17|v03h18|v03h19|v03h20|v03h21|v03h22|v03h23|" +
                "v04h15|v04h16|v04h17|v04h18|v04h19|v04h20|v04h21|v04h22|v04h23|" +
                "v05h15|v05h16|v05h17|v05h18|v05h19|v05h20|v05h21|v05h22|v05h23|" +
                "v06h15|v06h16|v06h17|v06h18|v06h19|v06h20|v06h21|v06h22|v06h23)" +
                ".*200206.*tif";
        Assert.assertEquals(expected, MerisPixelInputFormat.getInputPathPattern("2002", "06", new MerisStrategy().getArea("EUROPE")));
    }

    @Test
    public void getLcInputPathPatterns() throws Exception {

        String expected = "hdfs://calvalus/calvalus/projects/fire/aux/lc/lc-2000-(v00h15|v00h16|v00h17|v00h18|v00h19|v00h20|v00h21|v00h22|v00h23|" +
                "v01h15|v01h16|v01h17|v01h18|v01h19|v01h20|v01h21|v01h22|v01h23|" +
                "v02h15|v02h16|v02h17|v02h18|v02h19|v02h20|v02h21|v02h22|v02h23|" +
                "v03h15|v03h16|v03h17|v03h18|v03h19|v03h20|v03h21|v03h22|v03h23|" +
                "v04h15|v04h16|v04h17|v04h18|v04h19|v04h20|v04h21|v04h22|v04h23|" +
                "v05h15|v05h16|v05h17|v05h18|v05h19|v05h20|v05h21|v05h22|v05h23|" +
                "v06h15|v06h16|v06h17|v06h18|v06h19|v06h20|v06h21|v06h22|v06h23).*nc";

        Assert.assertEquals(expected, MerisPixelInputFormat.getLcInputPathPattern("2002", new MerisStrategy().getArea("EUROPE")));
    }

}