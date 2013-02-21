package com.bc.calvalus.portal.client.map;

import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.maps.client.LoadApi;
import com.google.gwt.maps.client.base.LatLng;

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
        Region region = new Region("world", new String[]{"user"},
                                   "polygon ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))");
        assertEquals("world", region.getName());
        assertEquals("user", region.getPath()[0]);
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
        Region region = new Region("world", new String[]{"user"}, new LatLng[]{
                LatLng.newInstance(-90, -180),
                LatLng.newInstance(-90, 180),
                LatLng.newInstance(90, 180),
                LatLng.newInstance(90, -180),
                LatLng.newInstance(-90, -180)
        });
        assertEquals("world", region.getName());
        assertEquals("user", region.getPath()[0]);
        assertEquals("user.world", region.getQualifiedName());
        assertEquals("POLYGON((-180.0 -90.0,180.0 -90.0,180.0 90.0,-180.0 90.0,-180.0 -90.0))",
                     region.getGeometryWkt());
    }

    public void testPath() {
        Region region = new Region("caribbean", new String[]{"marco", "peters", "zuehlke"}, new LatLng[0]);
        assertEquals("caribbean", region.getName());
        assertEquals(3, region.getPath().length);
        assertEquals("marco", region.getPath()[0]);
        assertEquals("peters", region.getPath()[1]);
        assertEquals("zuehlke", region.getPath()[2]);
        assertEquals("marco.peters.zuehlke.caribbean", region.getQualifiedName());
    }

    public void testSetName() {
        Region region = new Region("caribbean", new String[]{"marco", "peters", "zuehlke"}, new LatLng[0]);
        assertEquals("caribbean", region.getName());
        assertEquals(3, region.getPath().length);
        assertEquals("marco", region.getPath()[0]);
        assertEquals("peters", region.getPath()[1]);
        assertEquals("zuehlke", region.getPath()[2]);
        assertEquals("marco.peters.zuehlke.caribbean", region.getQualifiedName());
    }

    public void testSetQualifiedName() {
        Region region = new Region("caribbean", new String[]{"marco", "peters", "zuehlke"}, new LatLng[0]);
        String qualifiedName = "abc.def.ghi";
        region.setQualifiedName(qualifiedName);
        assertEquals("ghi", region.getName());
        assertEquals("abc", region.getPath()[0]);
        assertEquals("def", region.getPath()[1]);
        assertEquals("abc.def.ghi", region.getQualifiedName());

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
