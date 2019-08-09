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


import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

import java.util.ServiceLoader;

/**
 * The configuration for the match-up analysis.
 *
 * @author Norman
 */
public class MAConfig implements XmlConvertible {


    public static class VariableMapping {
        @Parameter
        private String reference;
        @Parameter
        private String satellite;

        // empty constructor for XML serialization
        public VariableMapping() {
        }

        public VariableMapping(String reference, String satellite) {
            this.reference = reference;
            this.satellite = satellite;
        }

        public String getReference() {
            return reference;
        }

        public String getSatellite() {
            return satellite;
        }
    }

    /**
     * If {@code copyInput = true}, all fields of an input (reference) record will be
     * copied into a corresponding output record.
     */
    @Parameter(defaultValue = "true")
    private boolean copyInput;

    /**
     * Size of the macro pixel given as number {@code n} of 'normal' pixels. An area comprising
     * {@code n x n} pixels will be considered in the match-up process. Should be an odd integer,
     * so that {@code n/2 - 1} pixels are considered around a given center pixel.
     * The default value size is {@code 5} pixels so that an area of 5 x 5 pixels will be considered.
     */
    @Parameter(defaultValue = "5")
    private int macroPixelSize;

    /**
     * If {@code onlyExtractComplete = true}, only macro pixels that comprise the complete {@code n x n} area
     * are extracted.
     */
    @Parameter(defaultValue = "true")
    private boolean onlyExtractComplete;

    /**
     * Maximum time difference in hours between reference and EO pixel.
     * If {@code maxTimeDifference = null}, the criterion will not be used and match-ups are found for all times.
     * The default value is {@code 3.0} hours.
     */
    @Parameter(defaultValue = "3.0")
    private String maxTimeDifference;

    /**
     * The band maths expression that identifies the "good" pixels in the macro pixel.
     * If not given, the criterion will not be used, thus all pixels will be considered being "good".
     */
    @Parameter
    private String goodPixelExpression;

    /**
     * Coefficient for <i>filtered mean criterion</i>.
     * If {@code filteredMeanCoeff <= 0}, the criterion will not be used.
     * The default value is {@code 1.5}.
     */
    @Parameter(defaultValue = "1.5")
    private Double filteredMeanCoeff;

    /**
     * If set to {@code true} overlapping match-ups, within one data product, are removed.
     * Only the one closest in time to the in-situ data is preserved.
     */
    @Parameter(defaultValue = "false")
    private boolean filterOverlapping;

    /**
     * The band maths expression that identifies the "good" records in the macro pixel.
     * If not given, the criterion will not be used, thus all pixels will be considered being "good".
     */
    @Parameter
    private String goodRecordExpression;

    /**
     * The time format used in the output.
     * Default is {@code "dd-MMM-yyyy HH:mm:ss"} (as used by Envisat).
     */
    @Parameter(defaultValue = "dd-MMM-yyyy HH:mm:ss")
    private String outputTimeFormat;

    /**
     * The name of the attribute that is used for output grouping.
     * A scatter plot is generated for each variable in a group.
     * Default is {@code "Site"}, an attribute that is considered to provide the name of a site where
     * in-situ data has been measured.
     */
    @Parameter(defaultValue = "site")
    private String outputGroupName;

    /**
     * The name of a class that implements the {@link RecordSourceSpi} interface.
     * Instances of this class are used to create {@link RecordSource} objects which are in turn used to
     * provide {@link Record}s.
     */
    @Parameter
    private String recordSourceSpiClassName;

    /**
     * The URL of a {@link RecordSource}.
     * General parameter for many types of record sources.
     */
    @Parameter
    private String recordSourceUrl;

    /**
     * If set to {@code true} the product or product parts
     * that have been processed will be saved.
     */
    @Parameter(defaultValue = "false")
    private boolean saveProcessedProducts;

    @Parameter(itemAlias = "variableMapping")
    private VariableMapping[] variableMappings;

    // The following have been discussed but not yet decided.

    /**
     * The name of the input column that holds the latitudes.
     * General parameter for many types of record sources.
     * May not be given.
     */
    // @Parameter (defaultValue = "lat")
    // private String recordSourceColumnNameLat;

    /**
     * The name of the input column that holds the longitudes.
     * General parameter for many types of record sources.
     * May not be given.
     */
    // @Parameter (defaultValue = "lon")
    // private String recordSourceColumnNameLon;

    /**
     * The name of the input column that holds the time.
     * General parameter for many types of record sources.
     * May not be given.
     */
    // @Parameter (defaultValue = "time")
    // private String recordSourceColumnNameTime;

    /**
     * The input format used for the time.
     * General parameter for many types of record sources.
     * Default value is "yyyy-MM-dd HH:mm:ss".
     */
    // @Parameter (defaultValue = "yyyy-MM-dd HH:mm:ss")
    // private String recordSourceTimeFormat;
    public MAConfig() {
    }

    public static MAConfig get(Configuration conf) {
        String xml = conf.get(JobConfigNames.CALVALUS_MA_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException("Missing match-up analysis configuration '" + JobConfigNames.CALVALUS_MA_PARAMETERS + "'");
        }
        try {
            return fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid match-up analysis configuration: " + e.getMessage(), e);
        }
    }

    public static MAConfig fromXml(String xml) throws BindingException {
        return new ParameterBlockConverter().convertXmlToObject(xml, new MAConfig());
    }

    @Override
    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (ConversionException e) {
            throw new RuntimeException(e);
        }
    }

    public RecordSource createRecordSource() throws Exception {
        String className = getRecordSourceSpiClassName();
        RecordSourceSpi service;
        if (className != null) {
            service = RecordSourceSpi.getForClassName(className);
        } else {
            service = RecordSourceSpi.getForUrl(getRecordSourceUrl());
        }
        if (service != null) {
            return service.createRecordSource(getRecordSourceUrl());
        } else {
            if (className != null) {
                throw new IllegalStateException("record source reader service " + className + " of point data file not found");
            } else {
                ServiceLoader<RecordSourceSpi> loader = ServiceLoader.load(RecordSourceSpi.class, Thread.currentThread().getContextClassLoader());
                StringBuilder supportedExtensions = new StringBuilder();
                for (RecordSourceSpi spi : loader) {
                    for (String extension : spi.getAcceptedExtensions()) {
                        if (supportedExtensions.length() > 0) {
                            supportedExtensions.append(", ");
                        }
                        supportedExtensions.append(extension);
                    }
                }
                throw new IllegalArgumentException("no record source reader found for filename extension of " + getRecordSourceUrl()
                                                           + " point data file (one of " + supportedExtensions + " expected)");
            }
        }
    }

    public String getRecordSourceSpiClassName() {
        return recordSourceSpiClassName;
    }

    public void setRecordSourceSpiClassName(String recordSourceSpiClassName) {
        this.recordSourceSpiClassName = recordSourceSpiClassName;
    }

    public String getRecordSourceUrl() {
        return recordSourceUrl;
    }

    public void setRecordSourceUrl(String recordSourceUrl) {
        this.recordSourceUrl = recordSourceUrl;
    }

    public String getOutputTimeFormat() {
        return outputTimeFormat;
    }

    public void setOutputTimeFormat(String outputTimeFormat) {
        this.outputTimeFormat = outputTimeFormat;
    }

    public String getOutputGroupName() {
        return outputGroupName;
    }

    public void setOutputGroupName(String outputGroupName) {
        this.outputGroupName = outputGroupName;
    }

    public boolean getCopyInput() {
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

    public String getMaxTimeDifference() {
        return maxTimeDifference;
    }

    public void setMaxTimeDifference(String maxTimeDifference) {
        this.maxTimeDifference = maxTimeDifference;
    }

    public Double getFilteredMeanCoeff() {
        return filteredMeanCoeff;
    }

    public void setFilteredMeanCoeff(Double filteredMeanCoeff) {
        this.filteredMeanCoeff = filteredMeanCoeff;
    }

    public String getGoodPixelExpression() {
        return goodPixelExpression;
    }

    public void setGoodPixelExpression(String goodPixelExpression) {
        this.goodPixelExpression = goodPixelExpression;
    }

    public String getGoodRecordExpression() {
        return goodRecordExpression;
    }

    public void setGoodRecordExpression(String goodRecordExpression) {
        this.goodRecordExpression = goodRecordExpression;
    }

    public boolean getFilterOverlapping() {
        return filterOverlapping;
    }

    public void setFilterOverlapping(boolean filterOverlapping) {
        this.filterOverlapping = filterOverlapping;
    }

    public boolean getSaveProcessedProducts() {
        return saveProcessedProducts;
    }

    public void setSaveProcessedProducts(boolean saveProcessedProducts) {
        this.saveProcessedProducts = saveProcessedProducts;
    }

    public VariableMapping[] getVariableMappings() {
        return variableMappings != null ? variableMappings : new VariableMapping[0];
    }

    public void setVariableMappings(VariableMapping[] variableMappings) {
        this.variableMappings = variableMappings;
    }

    public boolean getOnlyExtractComplete() {
        return onlyExtractComplete;
    }

    public void setOnlyExtractComplete(boolean onlyExtractComplete) {
        this.onlyExtractComplete = onlyExtractComplete;
    }

    public static boolean isMaxTimeDifferenceValid(String maxTimeDifference) {
        if (maxTimeDifference == null || maxTimeDifference.trim().isEmpty()) {
            return true;
        }
        maxTimeDifference = maxTimeDifference.trim();
        if (maxTimeDifference.endsWith("d") && maxTimeDifference.length() >= 2) {
            String daysAsString = maxTimeDifference.substring(0, maxTimeDifference.length() - 1);
            try {
                int days = Integer.parseInt(daysAsString);
                return days >= 0;
            } catch (NumberFormatException nfe) {
                return false;
            }
        } else {
            try {
                return Double.parseDouble(maxTimeDifference) >= 0;
            } catch (NumberFormatException nfe) {
                return false;
            }
        }
    }
}
