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


import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;
import org.esa.snap.core.util.converters.JtsGeometryConverter;

/**
 * The configuration for the trend analysis.
 *
 * @author Norman
 */
public class TAConfig implements XmlConvertible {

    @Parameter(itemAlias = "region")
    RegionConfiguration[] regions;

    public TAConfig(RegionConfiguration... regions) {
        this.regions = regions;
    }

    public RegionConfiguration[] getRegions() {
        return regions;
    }

    public static TAConfig get(Configuration conf) {
        String xml = conf.get(JobConfigNames.CALVALUS_TA_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException("Missing trend analysis configuration '" + JobConfigNames.CALVALUS_TA_PARAMETERS + "'");
        }
        try {
            return fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid trend analysis configuration: " + e.getMessage(), e);
        }
    }

    public static TAConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new TAConfig());
    }

    @Override
    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (ConversionException e) {
            throw new RuntimeException(e);
        }
    }

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

}
