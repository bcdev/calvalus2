package com.bc.calvalus.processing.ma;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.esa.snap.core.datamodel.GeoPos;

/**
 * A record filters that uses a geometry to filter out records.
 *
 * @author Norman
 */
public class GeometryRecordFilter implements RecordFilter {
    private final Geometry geometry;
    private final GeometryFactory pointFactory;

    public GeometryRecordFilter(Geometry geometry) {
        this(geometry, new GeometryFactory());
    }

    public GeometryRecordFilter(Geometry geometry, GeometryFactory pointFactory) {
        this.geometry = geometry;
        this.pointFactory = pointFactory;
    }

    @Override
    public boolean accept(Record record) {
        GeoPos location = record.getLocation();
        return location != null
                && location.isValid()
                && geometry.contains(createPoint(location));
    }

    private Point createPoint(GeoPos location) {
        return pointFactory.createPoint(new Coordinate(location.lon, location.lat));
    }
}
