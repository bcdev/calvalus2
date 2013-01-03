/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.processing.ProcessorDescriptor} class.
 *
 * @author Norman
 */
public class DtoProcessorDescriptor implements IsSerializable {

    private String executableName;
    private String processorName;
    private String processorVersion;
    private String defaultParameter;
    private String bundleName;
    private String bundleVersion;
    private String descriptionHtml;
    private String[] inputProductTypes;
    private String outputProductType;
    private String[] outputFormats;
    private String defaultMaskExpression;
    private DtoProcessorVariable[] processorVariables;
    private DtoParameterDescriptor[] parameterDescriptors;
    private boolean isFormattingMandatory;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProcessorDescriptor() {
    }

    public DtoProcessorDescriptor(String executableName,
                                  String processorName,
                                  String processorVersion,
                                  String defaultParameter,
                                  String bundleName,
                                  String bundleVersion,
                                  String descriptionHtml,
                                  String[] inputProductTypes,
                                  String outputProductType,
                                  String[] outputFormats,
                                  boolean isFormattingMandatory,
                                  String defaultMaskExpression,
                                  DtoProcessorVariable[] processorVariables,
                                  DtoParameterDescriptor[] parameterDescriptors) {
        this.executableName = executableName;
        this.processorName = processorName;
        this.processorVersion = processorVersion;
        this.defaultParameter = defaultParameter;
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        this.descriptionHtml = descriptionHtml;
        this.inputProductTypes = inputProductTypes;
        this.outputProductType = outputProductType;
        this.outputFormats = outputFormats;
        this.isFormattingMandatory = isFormattingMandatory;
        this.defaultMaskExpression = defaultMaskExpression;
        this.processorVariables = processorVariables;
        this.parameterDescriptors = parameterDescriptors;
    }

    public String getExecutableName() {
        return executableName;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getProcessorVersion() {
        return processorVersion;
    }

    public String getDefaultParameter() {
        return defaultParameter;
    }

    public String getBundleName() {
        return bundleName;
    }

    public String getBundleVersion() {
        return bundleVersion;
    }

    public String getDescriptionHtml() {
        return descriptionHtml;
    }

    public String[] getInputProductTypes() {
        return inputProductTypes;
    }

    public String getOutputProductType() {
        return outputProductType;
    }

    public String[] getOutputFormats() {
        return outputFormats;
    }

    public DtoProcessorVariable[] getProcessorVariables() {
        return processorVariables;
    }

    public String getDefaultMaskExpression() {
        return defaultMaskExpression;
    }

    public boolean isFormattingMandatory() {
        return isFormattingMandatory;
    }

    public DtoParameterDescriptor[] getParameterDescriptors() {
        return parameterDescriptors;
    }
}
