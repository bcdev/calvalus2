package com.bc.calvalus.portal.client.map;

import com.bc.calvalus.portal.shared.GsRegion;
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
    public static List<Region> decodeRegions(GsRegion[] encodedRegions) {
        ArrayList<Region> regionList = new ArrayList<Region>();
        for (GsRegion encodedRegion : encodedRegions) {
            String geometryWkt = encodedRegion.getGeometryWkt();
            Overlay overlay = WKTParser.parse(geometryWkt);
            if (overlay instanceof Polygon) {
                Polygon polygon = (Polygon) overlay;
                polygon.setVisible(true);
                regionList.add(new Region(encodedRegion.getName(), geometryWkt, polygon));
            }
        }
        return regionList;
    }

}
