package com.bc.calvalus.portal.client.map;

import com.bc.calvalus.portal.shared.DtoRegion;

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
            regionList.add(new Region(encodedRegion.getName(),
                                      encodedRegion.getPath(),
                                      encodedRegion.getGeometryWkt()));
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
            encodedRegions[i] = new DtoRegion(region.getName(),
                                              region.getPath(),
                                              region.getGeometryWkt());
        }
        return encodedRegions;
    }
}
