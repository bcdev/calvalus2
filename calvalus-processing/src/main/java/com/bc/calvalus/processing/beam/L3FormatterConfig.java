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

package com.bc.calvalus.processing.beam;

import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.gpf.annotations.Parameter;

import java.text.MessageFormat;
import java.text.ParseException;

/**
 * The configuration of the L3 formatter
 */
public class L3FormatterConfig {

    public static class BandConfiguration {
        String index;
        String name;
        String v1;
        String v2;
    }

    @Parameter
    private String outputType;
    @Parameter
    private String outputFile;
    @Parameter
    private String outputFormat;
    @Parameter(itemAlias = "band")
    private BandConfiguration[] bands;
    @Parameter
    private String startTime;
    @Parameter
    private String endTime;

    public static L3FormatterConfig create(String formatterParameters) {
        L3FormatterConfig formatterConfig = new L3FormatterConfig();
        ProcessingConfiguration.loadFromXml(formatterParameters, formatterConfig);
        return formatterConfig;
    }

    public L3FormatterConfig() {
        // used by DOM converter
    }

    public L3FormatterConfig(String outputType,
                             String outputFile,
                             String outputFormat,
                             BandConfiguration[] bands,
                             String startTime,
                             String endTime) {
        this.outputType = outputType;
        this.outputFile = outputFile;
        this.outputFormat = outputFormat;
        this.bands = bands;
        this.startTime = startTime;
        this.endTime = endTime;
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

    public ProductData.UTC getStartTime() {
        return parseTime(startTime, "startTime");
    }

    public ProductData.UTC getEndTime() {
        return parseTime(endTime, "endTime");
    }

    private static ProductData.UTC parseTime(String timeString, String timeName) {
        if (timeString == null) {
            throw new IllegalArgumentException(MessageFormat.format("Parameter: {0} not given.", timeName));
        }
        try {
            return ProductData.UTC.parse(timeString, "yyyy-MM-dd");
        } catch (ParseException e) {
            throw new IllegalArgumentException("Illegal start date format.", e);
        }

    }
}
