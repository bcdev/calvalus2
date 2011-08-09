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
import org.esa.beam.framework.gpf.annotations.Parameter;

/**
 * The configuration for the match-up analysis.
 *
 * @author Norman
 */
public class MAConfig {

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
    }

    public RecordSource createRecordSource() throws Exception {
        RecordSourceSpi service = RecordSourceSpi.get(recordSourceSpiClassName);
        return service != null ? service.createRecordSource(recordSourceUrl) : null;
    }
}
