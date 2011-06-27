package com.bc.calvalus.portal.client.map;

import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.maps.client.Maps;
import com.google.gwt.maps.client.geom.LatLng;

public class RegionTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    @Override
    protected void gwtSetUp() throws Exception {
        assertMapsApiLoaded();
    }

    public void testGetVerticesParsesGeometryWkt() {
        Region region = new Region("world", "user", "polygon ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))");
        assertEquals("world", region.getName());
        assertEquals("user", region.getCategory());
        assertEquals("user.world", region.getQualifiedName());

        LatLng[] polygon = region.getVertices();
        assertNotNull(polygon);
        assertEquals(5, polygon.length);
        assertEquals(LatLng.newInstance(-90, -180), polygon[0]);
        assertEquals(LatLng.newInstance(-90, 180), polygon[1]);
        assertEquals(LatLng.newInstance(90, 180), polygon[2]);
        assertEquals(LatLng.newInstance(90, -180), polygon[3]);
        assertEquals(LatLng.newInstance(-90, -180), polygon[4]);
    }

    public void testGetGeometryWktFormatsVertices() {
        Region region = new Region("world", "user", new LatLng[] {
                LatLng.newInstance(-90, -180),
                LatLng.newInstance(-90, 180),
                LatLng.newInstance(90, 180),
                LatLng.newInstance(90, -180),
                LatLng.newInstance(-90, -180)
        });
        assertEquals("world", region.getName());
        assertEquals("user", region.getCategory());
        assertEquals("user.world", region.getQualifiedName());
        assertEquals("POLYGON((-180.0 -90.0,180.0 -90.0,180.0 90.0,-180.0 90.0,-180.0 -90.0))",
                     region.getGeometryWkt());
    }

    private static void assertEquals(LatLng expected, LatLng vertex) {
        assertEquals(expected.getLatitude(), vertex.getLatitude(), 1e-5);
        assertEquals(expected.getLongitude(), vertex.getLongitude(), 1e-5);
    }

    private static void assertMapsApiLoaded() {
        if (!Maps.isLoaded()) {
            final long t0 = System.currentTimeMillis();
            Maps.loadMapsApi("", "2", false, new Runnable() {
                @Override
                public void run() {
                    final long t1 = System.currentTimeMillis();
                    long l = t1 - t0;
                    System.out.println("MapsApi loaded after " + l + " ms");
                }
            });
        }
        while (!Maps.isLoaded()) {
            // wait until it is loaded
        }
    }
}
