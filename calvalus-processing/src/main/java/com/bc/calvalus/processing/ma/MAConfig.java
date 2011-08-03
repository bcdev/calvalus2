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
import org.esa.beam.framework.gpf.annotations.Parameter;

import java.util.ServiceLoader;

/**
 * The configuration for the match-up analysis.
 *
 * @author Norman
 */
public class MAConfig {

    @Parameter
    private String recordSourceSpiClassName;

    public static MAConfig fromXml(String xml) {
        MAConfig config = new MAConfig();
        BeamUtils.convertXmlToObject(xml, config);
        return config;
    }

    public MAConfig() {
    }

    public MAConfig(String recordSourceSpiClassName) {
        this.recordSourceSpiClassName = recordSourceSpiClassName;
    }

    public RecordSource createRecordSource() {
        ServiceLoader<RecordSourceSpi> loader = ServiceLoader.load(RecordSourceSpi.class, Thread.currentThread().getContextClassLoader());
        loader.reload();
        for (RecordSourceSpi spi : loader) {
            if (spi.getClass().getName().equals(recordSourceSpiClassName)) {
                return spi.createRecordSource(this);
            }
        }
        return null;
    }
}
