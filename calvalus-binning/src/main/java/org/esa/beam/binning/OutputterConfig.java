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

package org.esa.beam.binning;

import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.framework.gpf.annotations.ParameterBlockConverter;

/**
 * The configuration of the L3 formatter
 */
public class OutputterConfig {

    public static class BandConfiguration {
        public String index;
        public String name;
        public String minValue;
        public String maxValue;
    }

    @Parameter(valueSet = {"Product", "RGB", "Grey"})
    private String outputType;
    @Parameter
    private String outputFile;
    @Parameter
    private String outputFormat;
    @Parameter(itemAlias = "band")
    private BandConfiguration[] bands;

    public OutputterConfig() {
        // used by DOM converter
    }

    public OutputterConfig(String outputType,
                           String outputFile,
                           String outputFormat,
                           BandConfiguration[] bands) {
        this.outputType = outputType;
        this.outputFile = outputFile;
        this.outputFormat = outputFormat;
        this.bands = bands;
    }

    /**
     * Creates a new formatter configuration object.
     *
     * @param xml The configuration as an XML string.
     * @return The new formatter configuration object.
     * @throws com.bc.ceres.binding.BindingException
     *          If the XML cannot be converted to a new formatter configuration object
     */
    public static OutputterConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new OutputterConfig());
    }

    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (ConversionException e) {
            throw new RuntimeException(e);
        }
    }

    public String getOutputType() {
        if (outputType == null) {
            throw new IllegalArgumentException("No output type given");
        }
        if (!outputType.equalsIgnoreCase("Product")
                && !outputType.equalsIgnoreCase("RGB")
                && !outputType.equalsIgnoreCase("Grey")) {
            throw new IllegalArgumentException("Unknown output type: " + outputType);
        }
        return outputType;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public BandConfiguration[] getBands() {
        return bands;
    }
}
