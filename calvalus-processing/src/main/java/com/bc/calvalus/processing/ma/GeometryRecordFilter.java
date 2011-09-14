package com.bc.calvalus.processing.ma;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.esa.beam.framework.datamodel.GeoPos;

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
