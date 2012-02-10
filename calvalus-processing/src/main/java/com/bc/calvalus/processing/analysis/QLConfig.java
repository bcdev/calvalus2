/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.framework.gpf.annotations.ParameterBlockConverter;

/**
 * Configuration for quick look generation
 */
class QLConfig {
    @Parameter
    int subSamplingX;
    @Parameter
    int subSamplingY;
    @Parameter
    String[] RGBAExpressions;
    @Parameter
    double[] v1;
    @Parameter
    double[] v2;
    @Parameter
    String imageType;

    public static QLConfig get(Configuration jobConfig) {
        String xml = jobConfig.get(JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException("Missing configuration '" + JobConfigNames.CALVALUS_QUICKLOOK_PARAMETERS + "'");
        }
        try {
            return fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid configuration: " + e.getMessage(), e);
        }
    }

    public static QLConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new QLConfig());
    }
}
