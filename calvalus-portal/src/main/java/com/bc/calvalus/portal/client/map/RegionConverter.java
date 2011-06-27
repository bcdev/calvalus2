package com.bc.calvalus.portal.client.map;

import com.bc.calvalus.portal.shared.DtoRegion;
import com.google.gwt.maps.client.overlay.Overlay;
import com.google.gwt.maps.client.overlay.Polygon;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts regions from their encoded forms to decoded counterparts and vice versa.
 *
 * @author Norman Fomferra
 */
public class RegionConverter {

    /**
     * @param encodedRegions The encoded regions.
     * @return A modifiable list of decoded regions.
     */
    public static List<Region> decodeRegions(DtoRegion[] encodedRegions) {
        ArrayList<Region> regionList = new ArrayList<Region>();
        for (DtoRegion encodedRegion : encodedRegions) {
            String geometryWkt = encodedRegion.getGeometryWkt();
            Overlay overlay = WKTParser.parse(geometryWkt);
            if (overlay instanceof Polygon) {
                Polygon polygon = (Polygon) overlay;
                regionList.add(new Region(encodedRegion.getName(), geometryWkt, Region.getPolygonVertices(polygon)));
            }
        }
        return regionList;
    }

    /**
     * @param regions The regions to encode.
     * @return The array of encoded regions.
     */
    public static DtoRegion[] encodeRegions(List<Region> regions) {
        DtoRegion[] encodedRegions = new DtoRegion[regions.size()];
        for (int i = 0; i < encodedRegions.length; i++) {
            Region region = regions.get(i);
            encodedRegions[i] = new DtoRegion(region.getName(), region.getPolygonWkt());
        }
        return encodedRegions;
    }
}
