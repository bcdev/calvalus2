package com.bc.calvalus.processing.fire.format.grid.avhrr;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class AvhrrGridMapperTest {

    private static double DEVIATION = 0.05;

    /*
    according to mail from Gonzalo:

        Raw=320, column = 769, Current  result = 6338184, Gonzalo’s result = 30161896.
        Raw=315, column = 661, Current result = 7206075, Gonzalo’s result = 17494142.

        Accept results with 5% deviation

    */

    @Test
    public void testGetStandardError() {
        double[] probBurn = new double[]{19.333332, 20.0, 12.333334, 15.666667, 15.166667, 16.666668, 23.5, 22.166666, 19.166666, 21.166668, 18.0, 20.166666, 19.166666, 16.166666, 15.5, 16.666668, 13.500001, 9.666666, 6.166667, 8.666667, 14.833333, 17.833332, 62.166668, 19.833334, 10.0};
        double[] probBurn_by100 = Arrays.stream(probBurn).map(d -> d / 100.0).toArray();
        assertEquals(30161896, new AvhrrGridMapper().getErrorPerPixel(probBurn_by100, 7.630269896117187E8, 0.22865), DEVIATION * 30161896);
    }

    @Test
    public void testGetStandardError2() {
        double[] probBurn = new double[]{42.0, 22.0, 23.166666, 24.833334, 53.666668, 18.0, 31.666666, 31.0, 32.666664, 27.000002, 10.5, 32.5, 28.166666, 63.333332, 48.5, 9.333333, 32.666664, 25.0, 30.000002, 19.833334, 17.333334, 38.833332, 46.5, 28.5, 26.0};
        double[] probBurn_by100 = Arrays.stream(probBurn).map(d -> d / 100.0).toArray();
        assertEquals(17494142, new AvhrrGridMapper().getErrorPerPixel(probBurn_by100, 7.599478154723896E8, 0.0443), DEVIATION * 17494142);

    }

}