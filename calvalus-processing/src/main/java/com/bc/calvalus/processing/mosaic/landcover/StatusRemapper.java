/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.mosaic.landcover;


import org.apache.hadoop.conf.Configuration;

public class StatusRemapper {
    private final int[] statusesToLand;

    public StatusRemapper(int[] statusesToLand) {
        this.statusesToLand = statusesToLand;
    }

    public static StatusRemapper create(Configuration jobConf) {
        String asLandText = jobConf.get("calvalus.lc.remapAsLand");
        if (asLandText != null) {
            return create(asLandText);
        }
        return null;
    }

    public static StatusRemapper create(String asLandText) {
        String[] asLandSplits = asLandText.split(",");
        int[] statusesToLand = new int[asLandSplits.length];
        for (int i = 0; i < asLandSplits.length; i++) {
            statusesToLand[i] = Integer.parseInt(asLandSplits[i]);
        }
        return new StatusRemapper(statusesToLand);
    }

    public int[] getStatusesToLand() {
        return statusesToLand;
    }


    public int remap(int status) {
        for (int s : statusesToLand) {
            if (s == status) {
                return LCMosaicAlgorithm.STATUS_LAND;
            }
        }
        return status;
    }

    public static int remapStatus(StatusRemapper statusRemapper, int status) {
        if (status > LCMosaicAlgorithm.STATUS_CLOUD_SHADOW) {
            if (statusRemapper != null) {
                // re-map e.g. ucl-cloud, ucl-cloud-buffer and schiller-cloud ==> cloud
                statusRemapper.remap(status);
            } else {
                return LCMosaicAlgorithm.STATUS_CLOUD;
            }
        }
        return status;
    }
}
