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

package com.bc.calvalus.processing.ma;


import com.bc.calvalus.processing.beam.BeamUtils;
import com.bc.ceres.core.Assert;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.gpf.annotations.Parameter;

/**
 * The configuration for the match-up analysis.
 *
 * @author Norman
 */
public class MAConfig {
    /**
     * Size of the macro pixel given as number {@code n} of 'normal' pixels. A window comprising
     * {@code n x n} will be considered in the match-up process. Should be an odd integer,
     * so that {@code n/2 - 1} pixels are considered around a given center pixel.
     */
    @Parameter(defaultValue = "1")
    int macroPixelSize;

    /**
     *  If {@code extractMacroPixel = true}, all pixels comprising the macro pixel will be extracted.
     *  If {@code extractMacroPixel = false}, macro pixel values will be averaged.
     */
    @Parameter(defaultValue = "false")
    boolean extractMacroPixel;

    /**
     * Maximum time difference in hours between reference and EO pixel.
     * If {@code maxTimeDifference <= 0}, the criterion will not be used and match-ups are found for all times.
     */
    @Parameter(defaultValue = "5")
    int maxTimeDifference;

    /**
     * Threshold for the <i>NGP/NTP criterion</i>.
     * If {@code minNgpToNtpRatio = 0.0}, the criterion will not be used.
     */
    @Parameter
    double minNgpToNtpRatio;

    /**
     * Band name for <i>filtered mean criterion</i>.
     * If not given, the criterion will not be used.
     */
    @Parameter
    String filteredMeanBandName;

    /**
     * Coefficient for <i>filtered mean criterion</i>.
     */
    @Parameter
    double filteredMeanCoefficient;

    /**
     * The band maths expression that identifies the "good" pixels in the macro pixel.
     * If not given, the criterion will not be used, thus all pixels will be considered being "good".
     */
    @Parameter(defaultValue = "1")
    String goodPixelExpression;

    /**
     * The date format used in the output.
     * Default is {@code "dd-MMM-yyyy HH:mm:ss"} (as used by Envisat).
     */
    @Parameter
    private String exportDateFormat;

    @Parameter
    private String recordSourceSpiClassName;

    @Parameter
    private String recordSourceUrl;

    public static MAConfig fromXml(String xml) {
        MAConfig config = new MAConfig();
        BeamUtils.convertXmlToObject(xml, config);
        return config;
    }

    public MAConfig() {
    }

    public MAConfig(String recordSourceSpiClassName,
                    String recordSourceUrl) {
        Assert.notNull(recordSourceSpiClassName, "recordSourceSpiClassName");
        Assert.notNull(recordSourceUrl, "recordSourceUrl");
        this.recordSourceSpiClassName = recordSourceSpiClassName;
        this.recordSourceUrl = recordSourceUrl;
        this.exportDateFormat = ProductData.UTC.DATE_FORMAT_PATTERN;
    }

    public RecordSource createRecordSource() throws Exception {
        RecordSourceSpi service = RecordSourceSpi.get(recordSourceSpiClassName);
        return service != null ? service.createRecordSource(recordSourceUrl) : null;
    }

    public String getRecordSourceSpiClassName() {
        return recordSourceSpiClassName;
    }

    public String getRecordSourceUrl() {
        return recordSourceUrl;
    }

    public String getExportDateFormat() {
        return exportDateFormat;
    }
}
