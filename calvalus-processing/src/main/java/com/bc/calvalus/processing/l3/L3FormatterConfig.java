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
import org.esa.beam.binning.operator.FormatterConfig;
import org.esa.beam.framework.datamodel.ProductData;

import java.text.ParseException;

// todo - remove class (OutputterConfig should be a sufficient replacement) (nf, 2012-02-14)

/**
 * The configuration of the L3 formatter
 */
public class L3FormatterConfig implements XmlConvertible {

    private FormatterConfig formatterConfig;

    public L3FormatterConfig() {
        this(new FormatterConfig());
    }

    public L3FormatterConfig(String outputType,
                             String outputFile,
                             String outputFormat,
                             FormatterConfig.BandConfiguration[] bands) {
        this(new FormatterConfig(outputType,
                                 outputFile,
                                 outputFormat,
                                 bands));
    }


    private L3FormatterConfig(FormatterConfig formatterConfig) {
        this.formatterConfig = formatterConfig;
    }

    public FormatterConfig getFormatterConfig() {
        return formatterConfig;
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
        return new L3FormatterConfig(FormatterConfig.fromXml(xml));
    }

    @Override
    public String toXml() {
        return formatterConfig.toXml();
    }

    public static ProductData.UTC parseTime(String timeString) {
        try {
            return ProductData.UTC.parse(timeString, "yyyy-MM-dd");
        } catch (ParseException e) {
            throw new IllegalArgumentException("Illegal date format.", e);
        }
    }
}
