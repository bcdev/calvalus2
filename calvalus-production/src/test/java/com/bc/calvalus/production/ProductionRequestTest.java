package com.bc.calvalus.production;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.bc.calvalus.commons.DateRange;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.junit.*;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

public class ProductionRequestTest {

    @Test
    public void testConstructors() {
        ProductionRequest req = new ProductionRequest("typeA", "ewa");
        assertEquals("typeA", req.getProductionType());
        assertEquals("ewa", req.getUserName());
        assertNotNull(req.getParameters());
        assertEquals(0, req.getParameters().size());

        req = new ProductionRequest("typeB", "ewa",
                                    "a", "3", "b", "8");
        assertEquals("typeB", req.getProductionType());
        assertEquals("ewa", req.getUserName());
        assertNotNull(req.getParameters());
        assertEquals(2, req.getParameters().size());

        try {
            new ProductionRequest(null, "ewa");
            fail("Production type must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("", "ewa");
            fail("Production type must not be empty (it is used in the output directory name)");
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            new ProductionRequest("t1", null);
            fail("User name must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t1", "");
            fail("User name must not be empty (it is used in the output directory name)");
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", "ewa",
                                  (Map<String, String>) null);
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", "ewa",
                                  (String) null);
            fail("#production parameters must be a multiple of 2");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", "ewa",
                                  (String) null, (String) "A");
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
        try {
            new ProductionRequest("t2", "ewa",
                                  (String) "A", (String) null);
            fail("Production parameters must not be null");
        } catch (NullPointerException e) {
            // ok
        }
    }

    @Test
    public void testBasicParameterAccessors() throws ProductionException, ParseException {
        ProductionRequest req = new ProductionRequest("typeA", "ewa",
                                                      "s", "Calvalus rules!",
                                                      "b", "true",
                                                      "i", "4",
                                                      "f", "-2.3",
                                                      "d", "2011-07-13",
                                                      "g", "POINT(15.1 11.6)",
                                                      "processorBundles", "calopus-beam-1.0,/calvalus/home/martin/software/calopus-aggregators-1.1");

        GeometryFactory gf = new GeometryFactory();
        Date date = ProductionRequest.DATE_FORMAT.parse("2011-07-13");
        Point point = gf.createPoint(new Coordinate(15.1, 11.6));

        assertEquals("Calvalus rules!", req.getString("s"));
        assertEquals(true, req.getBoolean("b"));
        assertEquals(4, req.getInteger("i"));
        assertEquals(-2.3F, req.getFloat("f"), 1e-5F);
        assertEquals(-2.3, req.getDouble("f"), 1e-10);
        assertEquals(date, req.getDate("d"));
        assertEquals("calopus-beam-1.0,/calvalus/home/martin/software/calopus-aggregators-1.1", req.getString("processorBundles"));
        assertTrue(point.equalsExact(req.getGeometry("g")));

        assertEquals("Calvalus sucks?", req.getString("missingParam", "Calvalus sucks?"));
        assertEquals(true, req.getBoolean("missingParam", true));
        assertEquals(234, (int) req.getInteger("missingParam", 234));
        assertEquals(1.9F, req.getFloat("missingParam", 1.9F), 1e-5F);
        assertEquals(1.9, req.getDouble("missingParam", 1.9), 1e-10);
        assertSame(date, req.getDate("missingParam", date));
        assertSame(point, req.getGeometry("missingParam", point));

        assertNull(req.getString("missingParam", null));
        assertNull(req.getBoolean("missingParam", null));
        assertNull(req.getInteger("missingParam", null));
        assertNull(req.getFloat("missingParam", null));
        assertNull(req.getDouble("missingParam", null));
        assertNull(req.getDate("missingParam", null));
        assertNull(req.getGeometry("missingParam", null));

        try {
            req.getString("missingParam");
            fail("ProductionException expected");
        } catch (ProductionException e) {
            // ok
        }

    }


    @Test
    public void testGetRegionGeometry() throws ProductionException {
        ProductionRequest req;
        Geometry regionGeometry;

        try {
            req = new ProductionRequest("typeA", "ewa",
                                        "maxLon", "-20.0",
                                        "maxLat", "23.4");
            req.getRegionGeometry(null);
            fail("Incomplete definition of region geometry");
        } catch (ProductionException e) {
            // ok
        }

        //-60.0, 13.4, -20.0, 23.4
        req = new ProductionRequest("typeA", "ewa",
                                    "minLon", "-60.0",
                                    "maxLon", "-20.0",
                                    "minLat", "13.4",
                                    "maxLat", "23.4");
        regionGeometry = req.getRegionGeometry(null);
        assertTrue(regionGeometry instanceof Polygon);
        assertEquals("POLYGON ((-60 13.4, -20 13.4, -20 23.4, -60 23.4, -60 13.4))", regionGeometry.toString());

        req = new ProductionRequest("typeA", "ewa",
                                    "regionWKT", "POINT(-60.0 13.4)");
        regionGeometry = req.getRegionGeometry(null);
        assertTrue(regionGeometry instanceof Point);
        assertEquals("POINT (-60 13.4)", regionGeometry.toString());

        req = new ProductionRequest("typeA", "ewa",
                                    "regionWKT", "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))");
        regionGeometry = req.getRegionGeometry(null);
        assertTrue(regionGeometry instanceof Polygon);
        assertEquals("POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))", regionGeometry.toString());

        req = new ProductionRequest("typeA", "ewa");
        Point defaultGeometry = new GeometryFactory().createPoint(new Coordinate(1, 2));
        regionGeometry = req.getRegionGeometry(defaultGeometry);
        assertSame(defaultGeometry, regionGeometry);
    }

    @Test
    public void testGetStagingDirectoryWithoutRemoteUser() throws Exception {
        ProductionRequest request = new ProductionRequest("L2Plus", "systemUser");

        assertThat(request.getStagingDirectory("product-00"), equalTo("systemUser/product-00"));
    }

    @Test
    public void testGetStagingDirectoryWhenRemoteUserIsSystemUser() throws Exception {
        ProductionRequest request = new ProductionRequest("L2Plus", "systemUser",
                                                          "calvalus.wps.remote.user", "systemUser");

        assertThat(request.getStagingDirectory("product-00"), equalTo("systemUser/product-00"));
    }

    @Test
    public void testGetStagingDirectoryWithRemoteUser() throws Exception {
        ProductionRequest request = new ProductionRequest("L2Plus", "systemUser",
                                                          "calvalus.wps.remote.user", "remoteUser");

        assertThat(request.getStagingDirectory("product-00"), equalTo("systemUser/remoteUser/product-00"));
    }

    @Test
    public void testDateListParsing() throws ParseException, ProductionException {
        String dateList = "2002-03-19\n2003-02-28\t\t2002-05-21 " +
                          "\n2002-11-13     2003-01-15 2002-04-28 2003-04-29\n\t 2003-04-30 ";
        ProductionRequest productionRequest = new ProductionRequest("L2", "ewa", "dateList", dateList);


        Date[] dates = productionRequest.getDates("dateList", null);
        assertNotNull(dates);
        assertEquals(8, dates.length);
        assertEquals(ProductionRequest.DATE_FORMAT.parse("2002-03-19"), dates[0]);
        assertEquals(ProductionRequest.DATE_FORMAT.parse("2002-04-28"), dates[1]);
        assertEquals(ProductionRequest.DATE_FORMAT.parse("2002-05-21"), dates[2]);
        assertEquals(ProductionRequest.DATE_FORMAT.parse("2002-11-13"), dates[3]);
        assertEquals(ProductionRequest.DATE_FORMAT.parse("2003-01-15"), dates[4]);
        assertEquals(ProductionRequest.DATE_FORMAT.parse("2003-02-28"), dates[5]);
        assertEquals(ProductionRequest.DATE_FORMAT.parse("2003-04-29"), dates[6]);
        assertEquals(ProductionRequest.DATE_FORMAT.parse("2003-04-30"), dates[7]);
    }

    @Test
    public void testCreateFromMinMax_wrongUsage() throws Exception {
        try {
            new ProductionRequest("test", "dummy").createFromMinMax();
            fail();
        } catch (ProductionException pe) {
            assertEquals("Production parameter 'minDate' not set.", pe.getMessage());
        }
        try {
            new ProductionRequest("test", "dummy", "minDate", "2001-02-03").createFromMinMax();
            fail();
        } catch (ProductionException pe) {
            assertEquals("Production parameter 'maxDate' not set.", pe.getMessage());
        }
    }

    @Test
    public void testCreateFromMinMax() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest("test", "dummy", "minDate", "2001-02-03", "maxDate",
                                                                    "2002-04-06");
        DateRange dateRange = productionRequest.createFromMinMax();
        assertNotNull(dateRange);
        assertNotNull(dateRange.getStartDate());
        assertNotNull(dateRange.getStopDate());
        assertEquals("2001-02-03", ProductionRequest.getDateFormat().format(dateRange.getStartDate()));
        assertEquals("2002-04-06", ProductionRequest.getDateFormat().format(dateRange.getStopDate()));
    }

    @Test
    public void testToXml() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest("test", "dummy", "minDate", "2001-02-03", "maxDate",
                                                                    "2002-04-06");
        String xmlString = productionRequest.toXml();
        assertEquals("<parameters>\n" +
                     "  <productionType>test</productionType>\n" +
                     "  <userName>dummy</userName>\n" +
                     "  <productionParameters class=\"java.util.HashMap\">\n" +
                     "    <minDate>2001-02-03</minDate>\n" +
                     "    <maxDate>2002-04-06</maxDate>\n" +
                     "  </productionParameters>\n" +
                     "</parameters>", xmlString);
    }

    @Test
    public void testFromXml() throws Exception {
        ProductionRequest productionRequest = ProductionRequest.fromXml("<parameters>\n" +
                                                                        "  <productionType>test</productionType>\n" +
                                                                        "  <userName>dummy</userName>\n" +
                                                                        "  <productionParameters class=\"java.util.HashMap\">\n" +
                                                                        "    <maxDate>2002-04-06</maxDate>\n" +
                                                                        "    <minDate>2001-02-03</minDate>\n" +
                                                                        "  </productionParameters>\n" +
                                                                        "</parameters>");
        DateRange dateRange = productionRequest.createFromMinMax();
        assertNotNull(dateRange);
        assertNotNull(dateRange.getStartDate());
        assertNotNull(dateRange.getStopDate());
        assertEquals("2001-02-03", ProductionRequest.getDateFormat().format(dateRange.getStartDate()));
        assertEquals("2002-04-06", ProductionRequest.getDateFormat().format(dateRange.getStopDate()));
    }

    @Test
    public void testFromXmlWithNull() throws Exception {
        assertNull(ProductionRequest.fromXml(null));
    }
}
