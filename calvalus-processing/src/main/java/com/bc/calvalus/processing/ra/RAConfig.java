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
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

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


    @Parameter(itemAlias = "region")
    private Region[] regions;

    @Parameter
    private String validExpressions;

    @Parameter
    private String[] bandNames;


    public RAConfig() {
    }

    public Region[] getRegions() {
        return regions;
    }

    public String getValidExpressions() {
        return validExpressions;
    }

    public String[] getBandNames() {
        return bandNames;
    }

    void setRegions(Region[] regions) {
        this.regions = regions;
    }

    void setValidExpressions(String validExpressions) {
        this.validExpressions = validExpressions;
    }

    void setBandNames(String[] bandNames) {
        this.bandNames = bandNames;
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
}
