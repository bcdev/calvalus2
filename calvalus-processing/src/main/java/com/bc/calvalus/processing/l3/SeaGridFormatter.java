/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.l2.ProductFormatter;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.TemporalBinSource;
import org.esa.snap.binning.operator.formatter.Formatter;
import org.esa.snap.binning.operator.formatter.FormatterConfig;
import org.esa.snap.binning.support.SEAGrid;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.ProductData;
import org.locationtech.jts.geom.Geometry;

import java.io.File;
import java.util.logging.Logger;

/**
 * Formats a SNAP {@link SEAGrid} as the flattened NetCDF layout required by
 * {@link ProductFormatter#FORMAT_NETCDF4_SEAGRID}.
 */
final class SeaGridFormatter implements Formatter {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void format(PlanetaryGrid planetaryGrid,
                       TemporalBinSource temporalBinSource,
                       String[] featureNames,
                       FormatterConfig formatterConfig,
                       Geometry regionGeometry,
                       ProductData.UTC startTime,
                       ProductData.UTC endTime,
                       MetadataElement... metadataElements) throws Exception {
        if (!(planetaryGrid instanceof SEAGrid)) {
            throw new IllegalArgumentException(
                    ProductFormatter.FORMAT_NETCDF4_SEAGRID +
                    " requires org.esa.snap.binning.support.SEAGrid, but the request uses " +
                    planetaryGrid.getClass().getName());
        }

        LOG.info("Using flattened SEAGrid NetCDF formatter for output format " +
                 ProductFormatter.FORMAT_NETCDF4_SEAGRID + '.');
        SeaGridNetcdfFormatter.write(new File(formatterConfig.getOutputFile()),
                                     (SEAGrid) planetaryGrid,
                                     temporalBinSource,
                                     featureNames,
                                     startTime,
                                     endTime,
                                     metadataElements);
    }
}
