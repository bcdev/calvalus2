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


import com.bc.calvalus.processing.xml.XmlBinding;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.core.Assert;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.gpf.annotations.Parameter;

/**
 * The configuration for the match-up analysis.
 *
 * @author Norman
 */
public class MAConfig implements XmlConvertible {
    /**
     * If {@code copyInput = true}, all fields of an input (reference) record will be
     * copied into a corresponding output record.
     */
    @Parameter(defaultValue = "true")
    boolean copyInput;

    /**
     * Size of the macro pixel given as number {@code n} of 'normal' pixels. A window comprising
     * {@code n x n} will be considered in the match-up process. Should be an odd integer,
     * so that {@code n/2 - 1} pixels are considered around a given center pixel.
     */
    @Parameter(defaultValue = "1")
    int macroPixelSize;

    /**
     * If {@code aggregateMacroPixel = true}, all 'good' macro pixel values will be aggregated (averaged).
     * If {@code aggregateMacroPixel = false}, all pixels comprising the macro pixel will be extracted.
     */
    @Parameter(defaultValue = "true")
    boolean aggregateMacroPixel;

    /**
     * Maximum time difference in hours between reference and EO pixel.
     * If {@code maxTimeDifference = null}, the criterion will not be used and match-ups are found for all times.
     */
    @Parameter
    Double maxTimeDifference;

    /**
     * The band maths expression that identifies the "good" pixels in the macro pixel.
     * If not given, the criterion will not be used, thus all pixels will be considered being "good".
     */
    @Parameter
    String goodPixelExpression;

    /**
     * The band maths expression that identifies the "good" records in the macro pixel.
     * If not given, the criterion will not be used, thus all pixels will be considered being "good".
     */
    @Parameter
    String goodRecordExpression;

    // Replaced by goodRecordExpression (nf, 2011-08-24)
    /**
     * Threshold for the <i>NGP/NTP criterion</i>.
     * If {@code minNgpToNtpRatio = 0.0}, the criterion will not be used.
     */
    //@Parameter
    //double minNgpToNtpRatio;

    // Replaced by goodRecordExpression (nf, 2011-08-24)
    /**
     * Band name for <i>filtered mean criterion</i>.
     * If not given, the criterion will not be used.
     */
    //@Parameter
    //String filteredMeanBandName;

    /**
     * Coefficient for <i>filtered mean criterion</i>.
     */
    @Parameter
    double filteredMeanCoeff;

    /**
     * The time format used in the output.
     * Default is {@code "dd-MMM-yyyy HH:mm:ss"} (as used by Envisat).
     */
    @Parameter
    private String outputTimeFormat;

    @Parameter
    private String recordSourceSpiClassName;

    @Parameter
    private String recordSourceUrl;


    public MAConfig() {
        setDefaults();
    }

    public MAConfig(String recordSourceSpiClassName,
                    String recordSourceUrl) {
        Assert.notNull(recordSourceSpiClassName, "recordSourceSpiClassName");
        Assert.notNull(recordSourceUrl, "recordSourceUrl");
        this.recordSourceSpiClassName = recordSourceSpiClassName;
        this.recordSourceUrl = recordSourceUrl;
        setDefaults();
    }

    public static MAConfig fromXml(String xml) {
        return new XmlBinding().convertXmlToObject(xml, new MAConfig());
    }

    @Override
    public String toXml()  {
        return new XmlBinding().convertObjectToXml(this);
    }

    public RecordSource createRecordSource() throws Exception {
        RecordSourceSpi service = RecordSourceSpi.get(recordSourceSpiClassName);
        return service != null ? service.createRecordSource(recordSourceUrl) : null;
    }

    private void setDefaults() {
        outputTimeFormat = ProductData.UTC.DATE_FORMAT_PATTERN;
        maxTimeDifference = null;
        macroPixelSize = 1;
        aggregateMacroPixel = true;
        copyInput = true;
    }

    public String getRecordSourceSpiClassName() {
        return recordSourceSpiClassName;
    }

    public String getRecordSourceUrl() {
        return recordSourceUrl;
    }

    public String getOutputTimeFormat() {
        return outputTimeFormat;
    }

    public boolean isCopyInput() {
        return copyInput;
    }

    public void setCopyInput(boolean copyInput) {
        this.copyInput = copyInput;
    }

    public int getMacroPixelSize() {
        return macroPixelSize;
    }

    public void setMacroPixelSize(int macroPixelSize) {
        this.macroPixelSize = macroPixelSize;
    }

    public boolean isAggregateMacroPixel() {
        return aggregateMacroPixel;
    }

    public void setAggregateMacroPixel(boolean aggregateMacroPixel) {
        this.aggregateMacroPixel = aggregateMacroPixel;
    }

    public Double getMaxTimeDifference() {
        return maxTimeDifference;
    }

    public void setMaxTimeDifference(Double maxTimeDifference) {
        this.maxTimeDifference = maxTimeDifference;
    }

    public double getFilteredMeanCoeff() {
        return filteredMeanCoeff;
    }

    public void setFilteredMeanCoeff(double filteredMeanCoeff) {
        this.filteredMeanCoeff = filteredMeanCoeff;
    }

    public String getGoodPixelExpression() {
        return goodPixelExpression;
    }

    public void setGoodPixelExpression(String goodPixelExpression) {
        this.goodPixelExpression = goodPixelExpression;
    }
}
