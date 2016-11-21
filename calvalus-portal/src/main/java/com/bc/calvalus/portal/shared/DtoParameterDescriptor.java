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
 * GWT-serializable version of the {@link com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor} class.
 *
 * @author Norman
 */
public class DtoParameterDescriptor implements IsSerializable {
    private String name;
    private String description;
    private String type;
    private String defaultValue;
    private String[] valueSet;
    private DtoValueRange valueRange;

    /**
     * No-arg constructor as required by {@link com.google.gwt.user.client.rpc.IsSerializable}. Don't use directly.
     */
    public DtoParameterDescriptor() {
    }

    public DtoParameterDescriptor(
            String name,
            String type,
            String description,
            String defaultValue,
            String[] valueSet,
            DtoValueRange valueRange) {
        this.type = type;
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.valueSet = valueSet;
        this.valueRange = valueRange;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getType() {
        return type;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String[] getValueSet() {
        return valueSet;
    }

    public DtoValueRange getValueRange() {
        return valueRange;
    }
}
