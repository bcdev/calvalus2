package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.util.math.MathUtils;
import org.geotools.referencing.CRS;

import javax.measure.quantity.Length;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;
import java.awt.geom.Rectangle2D;

/**
 * @author Marco Peters
 */
class AreaCalculator {
    private final GeoCoding gc;
    private double earthRadius;

    /**
     * Initialise the calculator with an {@link GeoCoding}.
     *
     * @param gc the geo-coding
     * @throws IllegalArgumentException Is thrown if the axis unit of the ellipsoid used by the
     *                                  underlying {@link GeoCoding#getMapCRS() map crs} is not
     *                                  {@code meter} or {@code kilometer}
     */
    AreaCalculator(GeoCoding gc) {
        this.gc = gc;
        Unit<Length> axisUnit = CRS.getEllipsoid(gc.getMapCRS()).getAxisUnit();
        double toMeter = getUnitConversionFactor(axisUnit);
        earthRadius = CRS.getEllipsoid(gc.getMapCRS()).getSemiMajorAxis() * toMeter;
    }

    /**
     * Calculates the size of the area of the rectangle specified. The unit of the size is either
     * {@code meter} or {@code kilometer} depending on the axis unit of the ellipsoid used by the geo-coding.
     * The rectangle needs to be specified in geo-graphical latitude/longitude coordinates
     *
     * @param rectangle rectangle of the area in latitude/longitude coordinates
     * @return the size in square meters
     */
    double calculateSize(Rectangle2D rectangle) {
        double deltaLon = rectangle.getWidth();
        double deltaLat = rectangle.getHeight();
        double centerLat = rectangle.getCenterY();
        double a = earthRadius * Math.cos(centerLat * MathUtils.DTOR) * deltaLon * MathUtils.DTOR;
        double b = earthRadius * deltaLat * MathUtils.DTOR;

        return (a * b);
    }

    /**
     * Creates a rectangle for the given pixel (x,y) using the specified geo-coding.
     *
     * @param x the x location of the pixel
     * @param y the y location of the pixel
     */
    Rectangle2D createGeoRectangleForPixel(int x, int y) {
        Rectangle2D.Double rect = new Rectangle2D.Double();
        GeoPos geoPosUL = gc.getGeoPos(new PixelPos(x, y), null);
        GeoPos geoPosLR = gc.getGeoPos(new PixelPos(x + 1, y + 1), null);
        rect.setFrameFromDiagonal(geoPosUL.getLon(), geoPosUL.getLat(),
                                  geoPosLR.getLon(), geoPosLR.getLat());
        return rect;
    }

    private double getUnitConversionFactor(Unit<Length> axisUnit) {
        double toMeter;
        if (axisUnit.equals(SI.METER)) {
            toMeter = 1.0;
        } else if (axisUnit.equals(SI.KILOMETER)) {
            toMeter = 1000.0;
        } else {
            throw new IllegalArgumentException("Earth axis must be specified either in meter or kilometer");
        }
        return toMeter;
    }

}
