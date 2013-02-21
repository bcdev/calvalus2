package com.bc.calvalus.portal.client.map;

import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.maps.client.LoadApi;
import com.google.gwt.maps.client.base.LatLng;

public class WKTParserTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    @Override
    protected void gwtSetUp() throws Exception {
        assertMapsApiLoaded();
    }

    public void testMarkerWKT() throws Exception {
        LatLng[] latLngs = WKTParser.parse("point(-18 -9)");
        assertNotNull(latLngs);
        assertEquals(1, latLngs.length);
        assertEquals(LatLng.newInstance(-9, -18), latLngs[0]);
    }

    public void testNormalisedWKT() throws Exception {
        assertWKTParsingOk("polygon((-180 -90, 180 -90, 180 90, -180 90, -180 -90))");
    }

    public void testWKTWithSomeWhites() throws Exception {
        assertWKTParsingOk("  POLYGON (\n" +
                                   "( -180 -90,\t180 -90,\t180\n" +
                                   "\n" +
                                   "90,\t-180 90\t  , -180 -90)\t)");
    }

    public void testParseWKTFailsForNull() throws Exception {
        try {
            WKTParser.parse(null);
            fail("NullPointerException expected!");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testParseWKTFailsForMalformedStrings() throws Exception {
        assertMalformedExceptionThrown("");
        assertMalformedExceptionThrown("BOLYGON((1 2, 3 4, 5 6, 1 2))");
        assertMalformedExceptionThrown("POLYGON(1 2, 3 4, 5 6, 1 2)");
        assertMalformedExceptionThrown("POLYGON(())");
        assertMalformedExceptionThrown("POLYGON((1 2, 3 4, 5 6, 1 2)");
        assertMalformedExceptionThrown("POLYGON(1 2, 3 4, 5 6, 1 2))");
        assertMalformedExceptionThrown("POLYGON(1, 2, 3, 4, 5, 6, 1, 2))");
    }

    private void assertMalformedExceptionThrown(String wkt) {
        try {
            WKTParser.parse(wkt);
            fail("IllegalArgumentException expected!");
        } catch (IllegalArgumentException e) {
            // ok, exception expected
            assertTrue("Unexpected message text: " + e.getMessage(),
                       e.getMessage().startsWith("Malformed WKT: "));
        }
    }

    public void testParseWKTFailsForNonWellFormedWKT() throws Exception {
        try {
            WKTParser.parse("");
            fail("IllegalArgumentException?");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    private void assertWKTParsingOk(String wkt) {
        LatLng[] latLngs = WKTParser.parse(wkt);
        assertEquals(5, latLngs.length);
        assertEquals(LatLng.newInstance(-90, -180), latLngs[0]);
        assertEquals(LatLng.newInstance(-90, 180), latLngs[1]);
        assertEquals(LatLng.newInstance(90, 180), latLngs[2]);
        assertEquals(LatLng.newInstance(90, -180), latLngs[3]);
        assertEquals(LatLng.newInstance(-90, -180), latLngs[4]);
    }

    private static void assertEquals(LatLng expected, LatLng vertex) {
        assertEquals(expected.getLatitude(), vertex.getLatitude(), 1e-5);
        assertEquals(expected.getLongitude(), vertex.getLongitude(), 1e-5);
    }


    private static void assertMapsApiLoaded() {
        final long t0 = System.currentTimeMillis();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                final long t1 = System.currentTimeMillis();
                long l = t1 - t0;
                System.out.println("MapsApi loaded after " + l + " ms");
            }
        };
        LoadApi.go(runnable, false);
    }
}
