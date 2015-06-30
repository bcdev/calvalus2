/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.framework.gpf.GPF;
import org.esa.snap.util.SystemUtils;

import javax.media.jai.JAI;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This class contains utility methods for dealing with BEAM and GPF
 */
public class GpfUtils {

    private static final int M = 1024 * 1024;
    public static final int DEFAULT_TILE_CACHE_SIZE = 512 * M; // 512 M

    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Initializes BEAM and GPF based on the configuration.
     *
     * @param configuration The Hadoop job configuration
     */
    public static void init(Configuration configuration) {
        initGpf(configuration, configuration.getClass());
    }

    public static void initGpf(Configuration configuration, Class aClass) {
        initSystemProperties(configuration);
        SystemUtils.init3rdPartyLibs(aClass);
        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(configuration.getLong(JobConfigNames.CALVALUS_BEAM_TILE_CACHE_SIZE, DEFAULT_TILE_CACHE_SIZE));
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }

    private static void initSystemProperties(Configuration configuration) {
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith("calvalus.system.")) {
                String propertyName = key.substring("calvalus.system.".length());
                String propertyValue = entry.getValue();
                LOG.info(String.format("Setting system property: %s=%s", propertyName, propertyValue));
                System.setProperty(propertyName, propertyValue);
            }
        }
    }
}
