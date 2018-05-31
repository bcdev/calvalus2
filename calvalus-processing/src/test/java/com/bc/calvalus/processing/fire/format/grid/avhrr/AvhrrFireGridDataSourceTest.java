package com.bc.calvalus.processing.fire.format.grid.avhrr;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class AvhrrFireGridDataSourceTest {

    @Test
    public void getPixelIndex() throws Exception {

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

}