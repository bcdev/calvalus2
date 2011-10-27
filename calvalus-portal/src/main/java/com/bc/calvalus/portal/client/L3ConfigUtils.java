package com.bc.calvalus.portal.client;

import com.google.gwt.maps.client.geom.LatLngBounds;

import java.util.Date;

import static java.lang.Math.PI;

/**
 * Utility functions used by L3Config.
 *
 * @author Norman
 */
public class L3ConfigUtils  {

    public static int getPeriodCount(Date minDate, Date maxDate, int steppingPeriodDays, int compositingPeriodDays) {
        long millisPerDay = 24L * 60L * 60L * 1000L;
        long deltaMillis = maxDate.getTime() - minDate.getTime() + millisPerDay;
        int deltaDays = (int) (deltaMillis / millisPerDay);
        int periodCount = deltaDays / steppingPeriodDays;
        int remainingDays = deltaDays % steppingPeriodDays;
        if (compositingPeriodDays <= remainingDays) {
            periodCount++;
        }
        return periodCount;
    }

    public static int[] getTargetSizeEstimation(LatLngBounds regionBounds, double res) {
        return getTargetSizeEstimation(regionBounds.getNorthEast().getLatitude() - regionBounds.getSouthWest().getLatitude(),
                                       regionBounds.getNorthEast().getLongitude() - regionBounds.getSouthWest().getLongitude(),
                                       res);

    }

    static int[] getTargetSizeEstimation(double deltaLat, double deltaLon, double res) {
        // see: SeaWiFS Technical Report Series Vol. 32;
        final double RE = 6378.145;
        int width = 1 + (int) Math.round((RE * PI * deltaLon / 180.0) / res);
        int height = 1 + (int) Math.round((RE * PI * deltaLat / 180.0) / res);
        return new int[]{width, height};
    }
}