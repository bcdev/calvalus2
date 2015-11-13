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
 * GWT-serializable version of the {@link com.bc.calvalus.processing.AggregatorDescriptor} class.
 *
 * @author Norman
 */
public class DtoAggregatorDescriptor implements IsSerializable {

    private String aggregator;
    private String bundleName;
    private String bundleVersion;
    private String bundleLocation;
    private String descriptionHtml;
    private DtoParameterDescriptor[] parameterDescriptors;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoAggregatorDescriptor() {
    }

    public DtoAggregatorDescriptor(String aggregator,
                                   String bundleName,
                                   String bundleVersion,
                                   String bundleLocation,
                                   String descriptionHtml,
                                   DtoParameterDescriptor[] parameterDescriptors) {
        this.aggregator = aggregator;
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        this.bundleLocation = bundleLocation;
        this.descriptionHtml = descriptionHtml;
        this.parameterDescriptors = parameterDescriptors;
    }

    public String getAggregator() {
        return aggregator;
    }

    public String getBundleName() {
        return bundleName;
    }

    public String getBundleVersion() {
        return bundleVersion;
    }

    public String getBundleLocation() {
        return bundleLocation;
    }

    public String getDescriptionHtml() {
        return descriptionHtml;
    }

    public DtoParameterDescriptor[] getParameterDescriptors() {
        return parameterDescriptors;
    }
}
