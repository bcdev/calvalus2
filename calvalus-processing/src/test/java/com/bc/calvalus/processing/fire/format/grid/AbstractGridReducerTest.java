package com.bc.calvalus.processing.fire.format.grid;

import junit.framework.TestCase;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class AbstractGridReducerTest extends TestCase {

    public void testGetFirstDayAsJD() {
        double jan = AbstractGridReducer.getFirstDayAsJD("2019", "01");
        double feb = AbstractGridReducer.getFirstDayAsJD("2019", "02");
        double mar = AbstractGridReducer.getFirstDayAsJD("2019", "03");
        double apr = AbstractGridReducer.getFirstDayAsJD("2019", "04");
        double may = AbstractGridReducer.getFirstDayAsJD("2019", "05");
        double jun = AbstractGridReducer.getFirstDayAsJD("2019", "06");
        double jul = AbstractGridReducer.getFirstDayAsJD("2019", "07");
        double aug = AbstractGridReducer.getFirstDayAsJD("2019", "08");
        double sep = AbstractGridReducer.getFirstDayAsJD("2019", "09");
        double oct = AbstractGridReducer.getFirstDayAsJD("2019", "10");
        double nov = AbstractGridReducer.getFirstDayAsJD("2019", "11");
        double dec = AbstractGridReducer.getFirstDayAsJD("2019", "12");
        assertEquals(31.0, feb-jan, 0.0001);
        assertEquals(28.0, mar-feb, 0.0001);
        assertEquals(31.0, apr-mar, 0.0001);
        assertEquals(30.0, may-apr, 0.0001);
        assertEquals(31.0, jun-may, 0.0001);
        assertEquals(30.0, jul-jun, 0.0001);
        assertEquals(31.0, aug-jul, 0.0001);
        assertEquals(31.0, sep-aug, 0.0001);
        assertEquals(30.0, oct-sep, 0.0001);
        assertEquals(31.0, nov-oct, 0.0001);
        assertEquals(30.0, dec-nov, 0.0001);
        assertEquals(17987.0, apr, 0.0001);
    }
}