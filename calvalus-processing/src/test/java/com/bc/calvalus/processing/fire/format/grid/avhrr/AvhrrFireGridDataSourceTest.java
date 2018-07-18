package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class AvhrrFireGridDataSourceTest {

    @Test
    public void testGetPixelIndex() throws Exception {

        // first target grid cell, first index

        assertEquals(0, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 0, 0));
        assertEquals(1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 1, 0, 0));
        assertEquals(4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 0, 0));
        assertEquals(7200, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 1, 0));
        assertEquals(7204, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 1, 0));
        assertEquals(28804, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 4, 0));

        // second target grid cell, first index

        assertEquals(5, AvhrrFireGridDataSource.getPixelIndex(1, 0, 0, 0, 0));
        assertEquals(6, AvhrrFireGridDataSource.getPixelIndex(1, 0, 1, 0, 0));
        assertEquals(9, AvhrrFireGridDataSource.getPixelIndex(1, 0, 4, 0, 0));
        assertEquals(7205, AvhrrFireGridDataSource.getPixelIndex(1, 0, 0, 1, 0));
        assertEquals(7209, AvhrrFireGridDataSource.getPixelIndex(1, 0, 4, 1, 0));
        assertEquals(28809, AvhrrFireGridDataSource.getPixelIndex(1, 0, 4, 4, 0));

        // rightmost target grid cell, first index

        assertEquals(5 * 79, AvhrrFireGridDataSource.getPixelIndex(79, 0, 0, 0, 0));
        assertEquals(5 * 79 + 1, AvhrrFireGridDataSource.getPixelIndex(79, 0, 1, 0, 0));
        assertEquals(5 * 79 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 0, 4, 0, 0));
        assertEquals(1 * 7200 + 5 * 79, AvhrrFireGridDataSource.getPixelIndex(79, 0, 0, 1, 0));
        assertEquals(1 * 7200 + 5 * 79 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 0, 4, 1, 0));
        assertEquals(4 * 7200 + 5 * 79 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 0, 4, 4, 0));

        // target grid cell at x0, y1, first index

        assertEquals(5 * 7200, AvhrrFireGridDataSource.getPixelIndex(0, 1, 0, 0, 0));
        assertEquals(5 * 7200 + 1, AvhrrFireGridDataSource.getPixelIndex(0, 1, 1, 0, 0));
        assertEquals(5 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 1, 4, 0, 0));
        assertEquals(5 * 7200 + 1 * 7200, AvhrrFireGridDataSource.getPixelIndex(0, 1, 0, 1, 0));
        assertEquals(5 * 7200 + 1 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 1, 4, 1, 0));
        assertEquals(5 * 7200 + 4 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 1, 4, 4, 0));


        // first target grid cell, second index

        assertEquals(400, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 0, 1));
        assertEquals(401, AvhrrFireGridDataSource.getPixelIndex(0, 0, 1, 0, 1));
        assertEquals(404, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 0, 1));
        assertEquals(7600, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 1, 1));
        assertEquals(7604, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 1, 1));
        assertEquals(29204, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 4, 1));

        // last target grid cell, second index

        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200, AvhrrFireGridDataSource.getPixelIndex(79, 79, 0, 0, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 1, AvhrrFireGridDataSource.getPixelIndex(79, 79, 1, 0, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 79, 4, 0, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 1 * 7200 + 0, AvhrrFireGridDataSource.getPixelIndex(79, 79, 0, 1, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 1 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 79, 4, 1, 1));
        assertEquals(1 * 400 + 79 * 5 + 79 * 5 * 7200 + 4 * 7200 + 4, AvhrrFireGridDataSource.getPixelIndex(79, 79, 4, 4, 1));


        // first target grid cell, upper rightmost index

        assertEquals(17 * 400 + 0, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 0, 17));
        assertEquals(17 * 400 + 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 1, 0, 17));
        assertEquals(17 * 400 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 0, 17));
        assertEquals(17 * 400 + 0 + 7200 * 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 1, 17));
        assertEquals(17 * 400 + 4 + 7200 * 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 1, 17));
        assertEquals(17 * 400 + 4 + 7200 * 4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 4, 17));

        // first target grid cell, second from above, left most index

        assertEquals(400 * 18 * 400 + 0, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 0, 18));
        assertEquals(7200 * 400 + 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 1, 0, 18));
        assertEquals(7200 * 400 + 4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 0, 18));
        assertEquals(7200 * 400 + 0 + 7200 * 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 0, 1, 18));
        assertEquals(7200 * 400 + 4 + 7200 * 1, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 1, 18));
        assertEquals(7200 * 400 + 4 + 7200 * 4, AvhrrFireGridDataSource.getPixelIndex(0, 0, 4, 4, 18));

        // last target grid cell, right-bottom index

        assertEquals(7200 * 3600 - 1, AvhrrFireGridDataSource.getPixelIndex(79, 79, 4, 4, 161));
    }

    @Test
    public void acceptanceTestGetData() throws Exception {
        Product porcProduct = ProductIO.readProduct("C:\\ssd\\avhrr\\BA_2000_2_Porcentage.tif");
        Product uncProduct = ProductIO.readProduct("C:\\ssd\\avhrr\\BA_2000_2_Uncertainty.tif");
        Product lcProduct = ProductIO.readProduct("C:\\ssd\\avhrr\\lc-avhrr.nc");
        for (int y = 0; y < 80; y++) {
            for (int x = 0; x < 80; x++) {
                SourceData data = new AvhrrFireGridDataSource(porcProduct, lcProduct, uncProduct, 113).readPixels(x, y);
            }
        }
    }

    @Test
    public void name() throws Exception {
        HashMap<String, Integer> map = new HashMap<>();

        for (String tileyear : tileyears) {
            String tile = tileyear.split("\\.")[0];
            int year = Integer.parseInt(tileyear.split("\\.")[1]);
            if (!map.containsKey(tile) || map.get(tile) > year) {
                map.put(tile, year);
            }
        }

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

    }


    String[] tileyears = new String[]{
            "h20v01.2015",
            "h20v01.2016",
            "h20v01.2017",
            "h21v01.2017",
            "h19v02.2004",
            "h19v02.2006",
            "h19v02.2007",
            "h19v02.2010",
            "h19v02.2012",
            "h19v02.2013",
            "h19v02.2014",
            "h19v02.2015",
            "h19v02.2016",
            "h19v02.2017",
            "h20v02.2015",
            "h20v02.2016",
            "h20v02.2017",
            "h21v02.2014",
            "h21v02.2015",
            "h21v02.2016",
            "h21v02.2017",
            "h22v02.2014",
            "h22v02.2016",
            "h22v02.2017",
            "h23v02.2013",
            "h23v02.2014",
            "h23v02.2015",
            "h23v02.2016",
            "h23v02.2017",
            "h24v02.2016",
            "h24v02.2017",
            "h20v03.2010",
            "h20v03.2011",
            "h20v03.2012",
            "h20v03.2013",
            "h20v03.2014",
            "h20v03.2015",
            "h20v03.2016",
            "h20v03.2017",
            "h21v03.2007",
            "h21v03.2008",
            "h21v03.2009",
            "h21v03.2010",
            "h21v03.2011",
            "h21v03.2012",
            "h21v03.2014",
            "h21v03.2015",
            "h21v03.2016",
            "h21v03.2017",
            "h22v03.2008",
            "h22v03.2009",
            "h22v03.2010",
            "h22v03.2011",
            "h22v03.2012",
            "h22v03.2013",
            "h22v03.2014",
            "h22v03.2015",
            "h22v03.2016",
            "h22v03.2017",
            "h23v03.2009",
            "h23v03.2010",
            "h23v03.2011",
            "h23v03.2012",
            "h23v03.2014",
            "h23v03.2015",
            "h23v03.2016",
            "h23v03.2017",
            "h24v03.2012",
            "h24v03.2013",
            "h24v03.2014",
            "h24v03.2015",
            "h24v03.2016",
            "h24v03.2017",
            "h25v03.2011",
            "h25v03.2012",
            "h25v03.2013",
            "h25v03.2014",
            "h25v03.2015",
            "h25v03.2016",
            "h25v03.2017",
            "h26v03.2017",
            "h21v04.2010",
            "h21v04.2011",
            "h21v04.2012",
            "h21v04.2013",
            "h21v04.2014",
            "h21v04.2015",
            "h21v04.2016",
            "h21v04.2017",
            "h22v04.2012",
            "h22v04.2013",
            "h22v04.2014",
            "h22v04.2015",
            "h22v04.2016",
            "h22v04.2017",
            "h23v04.2012",
            "h23v04.2013",
            "h23v04.2014",
            "h23v04.2016",
            "h23v04.2017",
            "h25v04.2012",
            "h25v04.2013",
            "h25v04.2015",
            "h25v04.2016",
            "h25v04.2017",
            "h26v04.2010",
            "h26v04.2011",
            "h26v04.2012",
            "h26v04.2013",
            "h26v04.2015",
            "h26v04.2016",
            "h26v04.2017",
            "h27v04.2013",
            "h27v04.2014",
            "h27v04.2015",
            "h27v04.2016",
            "h27v04.2017",
            "h21v05.2014",
            "h21v05.2015",
            "h21v05.2016",
            "h21v05.2017",
            "h27v05.2016",
            "h27v05.2017",
            "h24v06.2016",
            "h24v06.2017",
            "h25v06.2013",
            "h25v06.2014",
            "h25v06.2015",
            "h25v06.2016",
            "h25v06.2017",
            "h27v06.2016",
            "h27v06.2017",
            "h28v06.2017",
            "h25v07.2015",
            "h25v07.2016",
            "h25v07.2017",
            "h27v07.2010",
            "h27v07.2011",
            "h27v07.2012",
            "h27v07.2013",
            "h27v07.2014",
            "h27v07.2015",
            "h27v07.2016",
            "h27v07.2017",
            "h28v07.2012",
            "h28v07.2013",
            "h28v07.2014",
            "h28v07.2015",
            "h28v07.2016",
            "h28v07.2017",
            "h31v07.2000",
            "h32v07.2000",
            "h23v08.2000",
            "h28v09.2017",
            "h29v09.2016"
    };
}