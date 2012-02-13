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

import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import org.esa.beam.binning.OutputterConfig;
import org.esa.beam.framework.datamodel.ProductData;

/**
 * The configuration of the L3 formatter
 */
public class L3FormatterConfig implements XmlConvertible {

    private OutputterConfig outputterConfig;

    public L3FormatterConfig() {
        this(new OutputterConfig());
    }

    public L3FormatterConfig(String outputType,
                             String outputFile,
                             String outputFormat,
                             OutputterConfig.BandConfiguration[] bands,
                             // todo - remove
                             String startTime,
                             // todo - remove
                             String endTime) {
        this(new OutputterConfig(outputType,
                                 outputFile,
                                 outputFormat,
                                 bands,
                                 startTime,
                                 endTime));
    }


    private L3FormatterConfig(OutputterConfig outputterConfig) {
        this.outputterConfig = outputterConfig;
    }

    /**
     * Creates a new formatter configuration object.
     *
     * @param xml The configuration as an XML string.
     * @return The new formatter configuration object.
     * @throws com.bc.ceres.binding.BindingException
     *          If the XML cannot be converted to a new formatter configuration object
     */
    public static L3FormatterConfig fromXml(String xml) throws BindingException {
        return new L3FormatterConfig(OutputterConfig.fromXml(xml));
    }

    @Override
    public String toXml() {
        return outputterConfig.toXml();
    }

    public String getOutputType() {
        return outputterConfig.getOutputType();
    }

    public String getOutputFile() {
        return outputterConfig.getOutputFile();
    }

    public String getOutputFormat() {
        return outputterConfig.getOutputFormat();
    }

    public OutputterConfig.BandConfiguration[] getBands() {
        return outputterConfig.getBands();
    }

    // todo - remove
    public ProductData.UTC getStartTime() {
        return outputterConfig.getStartTime();
    }

    // todo - remove
    public ProductData.UTC getEndTime() {
        return outputterConfig.getEndTime();
    }
}
