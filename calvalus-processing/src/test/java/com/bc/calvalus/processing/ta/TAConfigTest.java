package com.bc.calvalus.processing.ta;


import com.bc.ceres.binding.BindingException;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TAConfigTest {

    private GeometryFactory factory;
    private TAConfig config;

    @Before
    public void setUp() throws Exception {
        factory = new GeometryFactory();
        Geometry geom1 = factory.createPoint(new Coordinate(10.1, 12.4));
        Geometry geom2 = factory.createPoint(new Coordinate(32.8, 19.9));
        config = new TAConfig(new TAConfig.RegionConfiguration("P1", geom1),
                                new TAConfig.RegionConfiguration("P2", geom2, null, -1000.0));
    }

    @Test
    public void testObjectCreation() {
        TAConfig.RegionConfiguration[] regions = config.getRegions();
        assertNotNull(regions);
        assertEquals(2, regions.length);
        assertEquals("P1", regions[0].getName());
        assertTrue(factory.createPoint(new Coordinate(10.1, 12.4)).equalsExact(regions[0].getGeometry()));
        assertNull(regions[0].getMinElevation());
        assertNull(regions[0].getMaxElevation());
        assertEquals("P2", regions[1].getName());
        assertTrue(factory.createPoint(new Coordinate(32.8, 19.9)).equalsExact(regions[1].getGeometry()));
        assertNull(regions[1].getMinElevation());
        assertEquals(new Double(-1000.0), regions[1].getMaxElevation());
    }

    @Test
    public void testXmlConversions() throws BindingException {
        String xml = config.toXml();
        assertNotNull(xml);

        TAConfig config2 = TAConfig.fromXml(xml);
        assertNotNull(config2);
        assertNotNull(config2.getRegions());
        assertEquals(2, config2.getRegions().length);
        for (int i = 0; i < 2; i++) {
            assertEquals(config.getRegions()[i].getName(), config2.getRegions()[i].getName());
            assertTrue(config.getRegions()[i].getGeometry().equalsExact(config2.getRegions()[i].getGeometry()));
            assertEquals(config.getRegions()[i].getMinElevation(), config2.getRegions()[i].getMinElevation());
            assertEquals(config.getRegions()[i].getMaxElevation(), config2.getRegions()[i].getMaxElevation());
        }
    }
}
