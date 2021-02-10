package com.bc.calvalus.processing.ma;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.esa.snap.core.datamodel.GeoPos;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class FilteredRecordSourceTest {

    @Test
    public void testGeometryFilter() throws Exception {


        DefaultRecordSource input = new DefaultRecordSource(new TestHeader("lat", "lon"),
                                                            RecordUtils.create(new GeoPos(51.0f, 13.8f), null),
                                                            RecordUtils.create(new GeoPos(53.0f, -120.5f), null),
                                                            RecordUtils.create(new GeoPos(50.5f, 10.5f), null),
                                                            RecordUtils.create(new GeoPos(50.5f, 0.0f), null),
                                                            RecordUtils.create(new GeoPos(-20.0f, 1.0f), null));
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(10.0, 50.0),
                new Coordinate(11.0, 50.0),
                new Coordinate(11.0, 51.0),
                new Coordinate(10.0, 51.0),
                new Coordinate(10.0, 50.0),
        }), null);

        FilteredRecordSource filteredRecordSource = new FilteredRecordSource(input, new GeometryRecordFilter(polygon, geometryFactory));

        Iterator<Record> iterator = filteredRecordSource.getRecords().iterator();

        assertTrue(iterator.hasNext());
        Record rec1 = iterator.next();
        assertNotNull(rec1);
        assertArrayEquals(RecordUtils.create(new GeoPos(50.5f, 10.5f), null).getAttributeValues(),
                          rec1.getAttributeValues());

        assertFalse(iterator.hasNext());
    }


}
