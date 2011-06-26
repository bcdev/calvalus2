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

package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.ProcessorDescriptor.Variable} class.
 *
 * @author MarcoZ
 */
public class DtoProcessorVariable implements IsSerializable {
    private String name;
    private String defaultAggregator;
    private String defaultValidMask;
    private String defaultWeightCoeff;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProcessorVariable() {
    }

    public DtoProcessorVariable(String name, String defaultAggregator, String defaultValidMask, String defaultWeightCoeff) {
        this.name = name;
        this.defaultAggregator = defaultAggregator;
        this.defaultValidMask = defaultValidMask;
        this.defaultWeightCoeff = defaultWeightCoeff;
    }

    public String getName() {
        return name;
    }

    public String getDefaultAggregator() {
        return defaultAggregator;
    }

    public String getDefaultValidMask() {
        return defaultValidMask;
    }

    public String getDefaultWeightCoeff() {
        return defaultWeightCoeff;
    }
}
