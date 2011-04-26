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

package com.bc.calvalus.processing.ta;


import com.bc.calvalus.processing.beam.BeamUtils;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.util.converters.JtsGeometryConverter;

/**
 * The configuration for the trend analysis.
 *
 * @author Norman
 */
public class TAConfig {


    public static class RegionConfiguration {

        @Parameter
        String name;
        @Parameter(converter = JtsGeometryConverter.class)
        Geometry geometry;
        @Parameter
        Double minElevation;
        @Parameter
        Double maxElevation;

        public RegionConfiguration() {
        }

        public RegionConfiguration(String name, Geometry geometry) {
            this.name = name;
            this.geometry = geometry;
        }

        public RegionConfiguration(String name, Geometry geometry, Double minElevation, Double maxElevation) {
            this.name = name;
            this.geometry = geometry;
            this.minElevation = minElevation;
            this.maxElevation = maxElevation;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Geometry getGeometry() {
            return geometry;
        }

        public void setGeometry(Geometry geometry) {
            this.geometry = geometry;
        }

        public Double getMinElevation() {
            return minElevation;
        }

        public void setMinElevation(Double minElevation) {
            this.minElevation = minElevation;
        }

        public Double getMaxElevation() {
            return maxElevation;
        }

        public void setMaxElevation(Double maxElevation) {
            this.maxElevation = maxElevation;
        }
    }

    @Parameter(itemAlias = "region")
    RegionConfiguration[] regions;

    public static TAConfig fromXml(String xml) {
        TAConfig config = new TAConfig();
        BeamUtils.convertXmlToObject(xml, config);
        return config;
    }

    public String toXml() {
        return BeamUtils.convertObjectToXml(this);
    }

    public TAConfig(RegionConfiguration... regions) {
        this.regions = regions;
    }

    public RegionConfiguration[] getRegions() {
        return regions;
    }
}
