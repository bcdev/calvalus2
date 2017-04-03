/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;
import org.geotools.index.CloseableIterator;

import java.io.IOException;

/**
 * The configuration for the region analysis workflow
 *
 * @author MarcoZ
 */
public class RAConfig implements XmlConvertible {


    public static class Region {
        @Parameter
        private String name;
        @Parameter
        private String wkt;

        // empty constructor for XML serialization
        public Region() {
        }

        public Region(String name, String wkt) {
            this.name = name;
            this.wkt = wkt;
        }

        public String getName() {
            return name;
        }

        public String getWkt() {
            return wkt;
        }
    }

    public static class BandConfig {
        @Parameter
        private String name;
        @Parameter
        private Integer numBins;
        @Parameter
        private Double lowValue;
        @Parameter
        private Double highValue;

        // empty constructor for XML serialization
        public BandConfig() {
        }

        public BandConfig(String name) {
            this(name, null, null, null);
        }

        public BandConfig(String name, Integer numBins, Double lowValue, Double highValue) {
            this.name = name;
            this.numBins = numBins;
            this.lowValue = lowValue;
            this.highValue = highValue;
        }

        public String getName() {
            return name;
        }

        public int getNumBins() {
            return numBins != null && lowValue != null && highValue != null ? numBins : 0;
        }

        public double getLowValue() {
            return lowValue != null ? lowValue : Double.NaN;
        }

        public double getHighValue() {
            return highValue != null ? highValue : Double.NaN;
        }
    }

    // TODO generate union of all regions for geo inventory query ( in ProductionType)

    // either regions or shapeFilePath has to be given
    @Parameter(itemAlias = "region")
    private Region[] regions;

    @Parameter
    private String shapeFilePath;

    // the filter* parameter are used to specify the shapefile attributes and the allowed values
    // if not given all geometries from the shapefile will be used
    @Parameter
    private String filterAttributeName;

    @Parameter
    private String filterAttributeValues;

    @Parameter
    private String validExpressions;

    // TODO bandNames have to be given, switch to all if not given ?
    @Parameter(itemAlias = "band")
    private BandConfig[] bands;

    @Parameter(defaultValue = "true")
    private boolean writeStatisticsFilePerRegion = true;

    @Parameter(defaultValue = "true")
    private boolean writeSeparateHistogramFile = true;


    // internal, will be set by the production type, to prevent repeated reading
    @Parameter
    private String[] internalRegionNames;


    public RAConfig() {
    }

    public void setRegions(Region...regions) {
        this.regions = regions;
    }

    public String[] getInternalRegionNames() {
        return internalRegionNames;
    }

    public void setInternalRegionNames(String...internalRegionNames) {
        this.internalRegionNames = internalRegionNames;
    }

    public String getValidExpressions() {
        return validExpressions;
    }

    public void setValidExpressions(String validExpressions) {
        this.validExpressions = validExpressions;
    }

    public BandConfig[] getBandConfigs() {
        return bands;
    }

    public void setBandConfigs(BandConfig...bandConfigs) {
        this.bands = bandConfigs;
    }

    public boolean isWriteStatisticsFilePerRegion() {
        return writeStatisticsFilePerRegion;
    }

    public void setWriteStatisticsFilePerRegion(boolean writeStatisticsFilePerRegion) {
        this.writeStatisticsFilePerRegion = writeStatisticsFilePerRegion;
    }

    public boolean isWriteSeparateHistogramFile() {
        return writeSeparateHistogramFile;
    }

    public void setWriteSeparateHistogramFile(boolean writeSeparateHistogramFile) {
        this.writeSeparateHistogramFile = writeSeparateHistogramFile;
    }

    public static RAConfig get(Configuration conf) {
        String xml = conf.get(JobConfigNames.CALVALUS_RA_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException("Missing region analysis configuration '" + JobConfigNames.CALVALUS_RA_PARAMETERS + "'");
        }
        try {
            return fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid region analysis configuration: " + e.getMessage(), e);
        }
    }

    public static RAConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new RAConfig());
    }

    @Override
    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (ConversionException e) {
            throw new RuntimeException(e);
        }
    }

    public static class NamedGeometry {

        public final String name;
        public final Geometry geometry;

        NamedGeometry(String name, Geometry geometry) {
            this.name = name;
            this.geometry = geometry;
        }
    }

    public CloseableIterator<NamedGeometry> createNamedRegionIterator(Configuration conf) throws IOException {
        if (shapeFilePath != null && !shapeFilePath.isEmpty()) {
            return new RARegions.RegionIteratorFromShapefile(shapeFilePath, filterAttributeName, filterAttributeValues, conf);
        } else if (regions != null) {
            return new RARegions.RegionIteratorFromWKT(regions);
        } else {
            throw new IllegalArgumentException("Neither regions nor a shapefile is given");
        }
    }
}
