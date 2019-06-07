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
        @SuppressWarnings("unused")
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
        private Double min;
        @Parameter
        private Double max;

        // empty constructor for XML serialization
        @SuppressWarnings("unused")
        public BandConfig() {
        }

        public BandConfig(String name) {
            this(name, null, null, null);
        }

        public BandConfig(String name, Integer numBins, Double min, Double max) {
            this.name = name;
            this.numBins = numBins;
            this.min = min;
            this.max = max;
        }

        public String getName() {
            return name;
        }

        public int getNumBins() {
            return numBins != null && min != null && max != null ? numBins : 0;
        }

        public double getMin() {
            return min != null ? min : Double.NaN;
        }

        public double getMax() {
            return max != null ? max : Double.NaN;
        }
    }

    // either regions or shapeFilePath has to be given
    @Parameter(itemAlias = "region")
    private Region[] regions;

    @Parameter
    private String regionSource;

    // the filter* parameter are used to specify the shapefile attributes and the allowed values
    // if not given all geometries from the shapefile will be used
    @Parameter
    private String regionSourceAttributeName;

    @Parameter
    private String regionSourceAttributeFilter;

    @Parameter(defaultValue = "true")
    private boolean withRegionEnvelope;

    @Parameter(defaultValue = "false")
    private boolean withProductNames;

    @Parameter
    private String goodPixelExpression;

    @Parameter(defaultValue = "")
    private int[] percentiles;

    // TODO bandNames have to be given, switch to all if not given ?
    @Parameter(itemAlias = "band")
    private BandConfig[] bands;

    @Parameter(defaultValue = "true")
    private boolean writePerRegion = true;

    @Parameter(defaultValue = "true")
    private boolean writeSeparateHistogram = true;

    // TODO
//    @Parameter(defaultValue = "false")
//    private boolean writeProductStatistics = false;

    @Parameter(defaultValue = "false")
    private boolean writePixelValues = false;

    @Parameter(defaultValue = "false")
    private boolean binValuesAsRatio = false;

    // internal, will be set by the production type, to prevent repeated reading
    @Parameter
    private String[] internalRegionNames;

    // empty constructor for XML serialization
    @SuppressWarnings("unused")
    public RAConfig() {
    }

    public void setRegions(Region... regions) {
        this.regions = regions;
    }

    public String[] getInternalRegionNames() {
        return internalRegionNames;
    }

    public void setInternalRegionNames(String... internalRegionNames) {
        this.internalRegionNames = internalRegionNames;
    }

    public void setRegionSource(String regionSource) {
        this.regionSource = regionSource;
    }

    public void setRegionSourceAttributeName(String regionSourceAttributeName) {
        this.regionSourceAttributeName = regionSourceAttributeName;
    }

    public void setRegionSourceAttributeFilter(String regionSourceAttributeFilter) {
        this.regionSourceAttributeFilter = regionSourceAttributeFilter;
    }

    public void setWithRegionEnvelope(boolean withRegionEnvelope) {
        this.withRegionEnvelope = withRegionEnvelope;
    }

    public boolean withRegionEnvelope() {return withRegionEnvelope; }

    public boolean withProductNames() {
        return withProductNames;
    }

    public void setWithProductNames(boolean withProductNames) {
        this.withProductNames = withProductNames;
    }

    public String getGoodPixelExpression() {
        return goodPixelExpression;
    }

    public void setGoodPixelExpression(String goodPixelExpression) {
        this.goodPixelExpression = goodPixelExpression;
    }

    public int[] getPercentiles() {
        return percentiles;
    }

    public void setPercentiles(int... percentiles) {
        this.percentiles = percentiles;
    }
    
    public void setPercentiles(String percentilesText) {
        if (percentilesText == null || percentilesText.trim().length() == 0) {
            this.percentiles = null;
        } else {
            String[] split = percentilesText.split(",");
            this.percentiles = new int[split.length];
            for (int i = 0; i < split.length; i++) {
                this.percentiles[i] = Integer.parseInt(split[i]);
            }
        }
    }

    public BandConfig[] getBandConfigs() {
        return bands;
    }

    public void setBandConfigs(BandConfig... bandConfigs) {
        this.bands = bandConfigs;
    }

    public boolean isWritePerRegion() {
        return writePerRegion;
    }

    public void setWritePerRegion(boolean writePerRegion) {
        this.writePerRegion = writePerRegion;
    }

    public boolean isWriteSeparateHistogram() {
        return writeSeparateHistogram;
    }

    public void setWriteSeparateHistogram(boolean writeSeparateHistogram) {
        this.writeSeparateHistogram = writeSeparateHistogram;
    }

//    public boolean isWriteProductStatistics() {
//        return writeProductStatistics;
//    }
//
//    public void setWriteProductStatistics(boolean writeProductStatistics) {
//        this.writeProductStatistics = writeProductStatistics;
//    }

    public boolean isWritePixelValues() {
        return writePixelValues;
    }

    public void setWritePixelValues(boolean writePixelValues) {
        this.writePixelValues = writePixelValues;
    }

    public boolean isBinValuesAsRatio() {
        return binValuesAsRatio;
    }

    public void setBinValuesAsRatio(boolean binValuesAsRatio) {
        this.binValuesAsRatio = binValuesAsRatio;
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

    public String[] getBandNames() {
        RAConfig.BandConfig[] bandConfigs = getBandConfigs();
        String[] bandNames = new String[bandConfigs.length];
        for (int i = 0; i < bandConfigs.length; i++) {
            RAConfig.BandConfig bConfig = bandConfigs[i];
            bandNames[i] = bConfig.getName().trim();
        }
        return bandNames;
    }

    public static class NamedRegion {

        public final String name;
        public final Geometry region;

        NamedRegion(String name, Geometry region) {
            this.name = name;
            this.region = region;
        }
    }

    public RARegions.RegionIterator createNamedRegionIterator(Configuration conf) throws IOException {
        if (regionSource != null && !regionSource.isEmpty()) {
            RARegions.RegionIterator regionIterator = new RARegions.RegionIteratorFromShapefile(regionSource, regionSourceAttributeName, conf);
            return new RARegions.FilterRegionIterator(regionIterator, regionSourceAttributeFilter);
        } else if (regions != null) {
            return new RARegions.RegionIteratorFromWKT(regions);
        } else {
            throw new IllegalArgumentException("Neither regions nor a regionSource is given");
        }
    }
}
